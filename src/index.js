import path from 'node:path';
import fs from 'fs-extra';
import zlib from 'node:zlib';
import { pipeline } from 'node:stream/promises';
import { promisify } from 'node:util';
import pLimit from 'p-limit';
import { splitFile, writeFileFromBlocks, createReadStreamFromBlocks } from './file.js';
import { hashBuffer } from './hasher.js';
import { writeBlocksBatch, readBlock } from './storage.js';
import { saveManifest, loadManifest } from './manifest.js';
import os from 'node:os';

const limit = pLimit(Math.max(4, os.cpus().length)); // Balanced concurrency
const deflateAsync = promisify(zlib.deflate);
const inflateAsync = promisify(zlib.inflate);
const brotliCompressAsync = promisify(zlib.brotliCompress);
const brotliDecompressAsync = promisify(zlib.brotliDecompress);

const DEFAULT_BROTLI_QUALITY = 2; // zlib.constants.BROTLI_PARAM_QUALITY is often 11 for general, 2 for fast
const DEFAULT_DEFLATE_LEVEL = zlib.constants.Z_BEST_SPEED;

const COMPRESSION_ALGORITHMS = {
  DEFLATE: 'deflate',
  BROTLI: 'brotli',
  NONE: 'none',
};

// Fast heuristic for compression
function shouldCompress(fileName, buffer) {
  if (buffer.length < 2048) return false;
  const ext = path.extname(fileName).toLowerCase();
  return !['.jpg', '.png', '.mp4', '.zip', '.gz', '.pdf', '.mp3', '.avi'].includes(ext);
}

/**
 * Store a file as blocks + manifest.
 * @param {string} filePath
 * @param {object} [options]
 * @returns {Promise<{ manifestPath: string, blockCount: number }>}
 */
export async function storeFile(filePath, {
  storageDir = './storage',
  blockSize = 8 * 1024 * 1024, // 8MB
  compress = true,
  verbose = false,
  compressionAlgorithm = COMPRESSION_ALGORITHMS.BROTLI,
  brotliQuality = DEFAULT_BROTLI_QUALITY,
  deflateLevel = DEFAULT_DEFLATE_LEVEL,
} = {}) {
  if (!(await fs.access(filePath).then(() => true).catch(() => false))) throw new Error(`File not found: ${filePath}`);
  if (blockSize <= 0) throw new Error('Block size must be positive');

  const fileName = path.basename(filePath);
  const blockDir = path.join(storageDir, 'blocks');
  const manifestDir = path.join(storageDir, 'manifests');
  await fs.mkdir(blockDir, { recursive: true });
  await fs.mkdir(manifestDir, { recursive: true });

  const stats = await fs.stat(filePath);
  if (stats.size === 0) {
    const metadata = {
      originalSize: 0,
      blockSize,
      compress: false,
      compressionAlgorithm: COMPRESSION_ALGORITHMS.NONE,
      originalName: fileName,
      createdAt: new Date().toISOString()
    };
    const manifestPath = path.join(manifestDir, `${fileName}.manifest.json`);
    await fs.writeJson(manifestPath, { hashes: [], blockCompression: [], metadata });
    return { manifestPath, blockCount: 0 };
  }

  // Fast path for small files (<8MB)
  if (stats.size <= blockSize) {
    const buffer = await fs.readFile(filePath);
    const { manifestPath, blockCount } = await writeFile(fileName, buffer, {
      storageDir,
      blockSize,
      compress,
      verbose,
      compressionAlgorithm
    });
    return { manifestPath, blockCount };
  }

  const metadata = {
    originalSize: stats.size,
    blockSize,
    compress,
    compressionAlgorithm: compress ? compressionAlgorithm : COMPRESSION_ALGORITHMS.NONE,
    originalName: fileName,
    createdAt: new Date().toISOString(),
    compressionParams: null, // Will be populated if compression happens
  };

  if (compress) {
    if (metadata.compressionAlgorithm === COMPRESSION_ALGORITHMS.BROTLI) {
      metadata.compressionParams = { quality: brotliQuality };
    } else if (metadata.compressionAlgorithm === COMPRESSION_ALGORITHMS.DEFLATE) {
      metadata.compressionParams = { level: deflateLevel };
    }
  }

  const stream = fs.createReadStream(filePath, { highWaterMark: blockSize });
  const hashes = [];
  const blockCompression = [];
  const blocksToWrite = [];
  let blockIndex = 0;
  const processingQueue = [];
  let isProcessingQueue = false;
  let streamPaused = false;
  const PROCESSING_QUEUE_HIGH_WATER_MARK = Math.max(8, os.cpus().length * 2); // Concurrency for processing
  const PROCESSING_QUEUE_LOW_WATER_MARK = Math.max(4, os.cpus().length);
  let streamEnded = false; // Flag to indicate that the 'end' event has fired

  async function processQueue() {
    if (isProcessingQueue) return;
    isProcessingQueue = true;

    try {
      while (processingQueue.length > 0) {
        const { chunk, blockIndex: originalChunkIndex } = processingQueue.shift();

        let processedBlock = chunk;
        let actualCompression = COMPRESSION_ALGORITHMS.NONE;

        if (compress && shouldCompress(fileName, chunk)) {
          actualCompression = metadata.compressionAlgorithm; // This is the target algorithm
          let currentCompressionParams = {};
          try {
            if (actualCompression === COMPRESSION_ALGORITHMS.BROTLI) {
              currentCompressionParams = { params: { [zlib.constants.BROTLI_PARAM_QUALITY]: brotliQuality } };
              processedBlock = await limit(() => brotliCompressAsync(chunk, currentCompressionParams));
            } else if (actualCompression === COMPRESSION_ALGORITHMS.DEFLATE) {
              currentCompressionParams = { level: deflateLevel };
              processedBlock = await limit(() => deflateAsync(chunk, currentCompressionParams));
            } else { // Should not happen if actualCompression is BROTLI or DEFLATE
              processedBlock = chunk;
              actualCompression = COMPRESSION_ALGORITHMS.NONE;
            }

            if (processedBlock.length >= chunk.length) { // If compression is ineffective
              processedBlock = chunk;
              actualCompression = COMPRESSION_ALGORITHMS.NONE;
              // No specific compressionParams if NONE
            }
          } catch (err) {
            if (verbose) console.warn(`Compression failed for block index ${originalChunkIndex} with ${actualCompression}, using original. Error: ${err.message}`);
            processedBlock = chunk;
            actualCompression = COMPRESSION_ALGORITHMS.NONE;
          }
        } else { // Not compressing this block
          actualCompression = COMPRESSION_ALGORITHMS.NONE;
        }

        const hash = hashBuffer(processedBlock);
        hashes.push(hash);
        blockCompression.push(actualCompression); // Store the actual compression used for this block

        blocksToWrite.push({ hash, data: processedBlock });

        if (blocksToWrite.length >= 8) {
          const batchToWrite = blocksToWrite.splice(0, 8);
          await limit(() => writeBlocksBatch(blockDir, batchToWrite, { verbose }));
        }

        if (streamPaused && processingQueue.length < PROCESSING_QUEUE_LOW_WATER_MARK) {
          stream.resume();
          streamPaused = false;
          if (verbose) console.log('Stream resumed, processing queue below low water mark.');
        }
      }
    } catch (err) {
      isProcessingQueue = false; // Ensure flag is reset on error
      stream.destroy(err); // Destroy stream with error to propagate to 'error' and 'close' events
      // Error will be caught by the .catch(reject) where processQueue() is invoked from 'data' or 'end' handler
      throw err;
    } finally {
      isProcessingQueue = false;
      // The 'end' handler is now responsible for ensuring all blocks are written after
      // the queue is empty and stream has ended.
    }
  }

  return new Promise((resolve, reject) => {
    stream.on('data', (chunk) => {
      if (chunk.length === 0) return;
      if (stream.destroyed) return; // Don't process if stream is already destroyed

      blockIndex++; // This counter is for the number of chunks read from stream
      processingQueue.push({ chunk, blockIndex: blockIndex - 1 });

      if (!isProcessingQueue) {
        processQueue().catch(processError => {
          // If processQueue encounters an error, it should call stream.destroy(error).
          // This will trigger the stream's 'error' event, which then rejects the main promise.
          // So, we just need to ensure stream is destroyed if it hasn't been already.
          if (!stream.destroyed) {
            stream.destroy(processError);
          }
          // Do not call reject() here; let the stream 'error' event handle it.
        });
      }

      if (processingQueue.length >= PROCESSING_QUEUE_HIGH_WATER_MARK && !streamPaused && !stream.destroyed) {
        stream.pause();
        streamPaused = true;
        if (verbose) console.log('Stream paused due to high processing queue');
      }
    });

    stream.on('end', async () => {
      if (verbose) console.log('Stream ended. Starting final processing and writes.');
      streamEnded = true; // Signal that stream has ended

      try {
        // Ensure any ongoing processing finishes or new processing is triggered if queue still has items
        if (!isProcessingQueue && processingQueue.length > 0) {
          await processQueue(); // Wait for it to complete
        } else {
          // If processQueue is already running, wait for it to naturally empty the queue.
          while (isProcessingQueue || processingQueue.length > 0) {
            await new Promise(r => setTimeout(r, 50));
          }
        }

        // After all processing is done, write any remaining blocks
        if (blocksToWrite.length > 0) {
          if (verbose) console.log(`Writing final ${blocksToWrite.length} blocks.`);
          const finalBatch = blocksToWrite.splice(0, blocksToWrite.length);
          await limit(() => writeBlocksBatch(blockDir, finalBatch, { verbose }));
        }

        // All data processed and written, finalize manifest
        const manifestPath = path.join(manifestDir, `${fileName}.manifest.json`);
        await fs.writeJson(manifestPath, { hashes, blockCompression, metadata });
        resolve({ manifestPath, blockCount: hashes.length });
      } catch (err) {
        // Ensure stream is destroyed on any error during 'end' processing
        if (!stream.destroyed) {
            stream.destroy(err);
        }
        reject(err);
      }
    });

    stream.on('error', (err) => {
        // This handler will catch errors from stream.destroy(err) calls elsewhere
        // and also direct stream errors.
        if (verbose) console.error('Stream error event:', err.message);
        // Clean up resources or ensure processing stops
        isProcessingQueue = false; // Stop any further processing attempts by new calls
        processingQueue.length = 0; // Clear queue
        blocksToWrite.length = 0; // Clear pending writes
        reject(new Error(`Stream or processing error: ${err.message}`));
    });
  });
}
      await fs.writeJson(manifestPath, { hashes, blockCompression, metadata });
      resolve({ manifestPath, blockCount: hashes.length });
    });

    stream.on('error', (err) => reject(new Error(`Stream error: ${err.message}`)));
  });
}

/**
 * Restore a file from manifest and blocks.
 * @param {string} manifestPath
 * @param {string} outputPath
 * @param {object} [options]
 * @returns {Promise<void>}
 */
export async function restoreFile(manifestPath, outputPath, {
  storageDir = './storage',
  verbose = false,
} = {}) {
  if (!(await fs.access(manifestPath).then(() => true).catch(() => false))) throw new Error(`Manifest file not found: ${manifestPath}`);
  const blockDir = path.join(storageDir, 'blocks');
  if (!(await fs.access(blockDir).then(() => true).catch(() => false))) throw new Error(`Block directory not found: ${blockDir}`);

  const { hashes, blockCompression, metadata } = await loadManifest(manifestPath);
  if (!Array.isArray(hashes) || !Array.isArray(blockCompression) || hashes.length !== blockCompression.length) {
    throw new Error('Invalid manifest: hashes or blockCompression is missing or inconsistent.');
  }

  if (hashes.length === 0 && metadata.originalSize === 0) {
    await fs.writeFile(outputPath, '');
    return;
  }

  // It's possible for a file to have content but result in zero blocks if all blocks were empty after processing (e.g. large sparse file)
  // However, the current storeFile logic does not produce zero hashes for non-empty originalSize.
  // This check is more about manifest integrity.
  if (hashes.length === 0 && metadata.originalSize > 0) {
    throw new Error('Manifest is invalid: no hashes for a non-empty file.');
  }


  const outputStream = fs.createWriteStream(outputPath);
  await pipeline(
    createReadStreamFromBlocks(blockDir, hashes, {
      blockCompression,
      verbose,
      // compress: metadata.compress, // Less relevant now, covered by compressionAlgorithm
      compressionAlgorithm: metadata.compressionAlgorithm,
      limit // Pass the pLimit instance
    }),
    outputStream
  );
}

/**
 * Reads logical file from manifest and blocks.
 * @param {string} manifestPath
 * @param {object} [options]
 * @returns {Promise<Buffer|Readable>}
 */
export async function readFile(manifestPath, {
  storageDir = './storage',
  verbose = false,
  stream = false,
} = {}) {
  if (!manifestPath.endsWith('.json')) throw new Error('Manifest path must point to a .json file');
  if (!(await fs.access(manifestPath).then(() => true).catch(() => false))) throw new Error(`Manifest file not found: ${manifestPath}`);

  const blockDir = path.join(storageDir, 'blocks');
  if (!(await fs.access(blockDir).then(() => true).catch(() => false))) throw new Error(`Block directory not found: ${blockDir}`);

  const { hashes, blockCompression, metadata } = await loadManifest(manifestPath);
  if (!Array.isArray(hashes) || !Array.isArray(blockCompression) || hashes.length !== blockCompression.length) {
    throw new Error('Invalid manifest: hashes or blockCompression is missing or inconsistent.');
  }
  if (hashes.length === 0 && metadata.originalSize > 0) { // Similar check as in restoreFile
    throw new Error('Manifest is invalid: no hashes for a non-empty file.');
  }


  if (stream) {
    return createReadStreamFromBlocks(blockDir, hashes, {
      // compress: metadata.compress, // Less relevant now, covered by compressionAlgorithm
      compressionAlgorithm: metadata.compressionAlgorithm,
      verbose,
      blockCompression,
      limit // Pass the pLimit instance
    });
  }

  const blockPromises = hashes.map((hash, i) => limit(async () => {
    const compression = blockCompression[i];
    const block = await readBlock(blockDir, hash);
    try {
      return compression !== COMPRESSION_ALGORITHMS.NONE
        ? compression === COMPRESSION_ALGORITHMS.BROTLI
          ? await brotliDecompressAsync(block)
          : await inflateAsync(block)
        : block;
    } catch (err) {
      throw new Error(`Decompression failed for block ${i + 1}: ${err.message}`);
    }
  }));

  return Buffer.concat(await Promise.all(blockPromises));
}

/**
 * Verifies a restored file against its manifest.
 * @param {string} filePath
 * @param {string} manifestPath
 * @param {object} [options]
 * @returns {Promise<boolean>}
 */
export async function verifyFile(filePath, manifestPath, {
  storageDir = './storage',
  verbose = false,
} = {}) {
  if (!(await fs.access(filePath).then(() => true).catch(() => false))) throw new Error(`File not found: ${filePath}`);
  if (!(await fs.access(manifestPath).then(() => true).catch(() => false))) throw new Error(`Manifest file not found: ${manifestPath}`);

  const { hashes: manifestHashes, blockCompression, metadata } = await loadManifest(manifestPath);
  if (!metadata.blockSize) throw new Error('Manifest missing blockSize metadata');
  if (!Array.isArray(manifestHashes) || manifestHashes.length === 0 && metadata.originalSize > 0) {
    throw new Error('Manifest contains no hashes for a non-empty file.');
  }
  if (metadata.originalSize === 0 && manifestHashes.length > 0) {
    throw new Error('Manifest contains hashes for an empty file.');
  }
  if (metadata.originalSize === 0 && manifestHashes.length === 0) {
    const actualFileStat = await fs.stat(filePath);
    if (actualFileStat.size !== 0) throw new Error(`Manifest indicates empty file, but file on disk has size ${actualFileStat.size}.`);
    if (verbose) console.log('File and manifest correctly represent an empty file.');
    return true;
  }

  const blockDir = path.join(storageDir, 'blocks');
  const fileStream = fs.createReadStream(filePath, { highWaterMark: metadata.blockSize });
  let currentFileBlockBuffer = Buffer.alloc(0);
  let manifestHashIndex = 0;
  let totalBytesReadFromFile = 0;

  return new Promise((resolve, reject) => {
    const processBlock = async (fileBlockChunk, isLastBlock = false) => {
      if (manifestHashIndex >= manifestHashes.length) {
        fileStream.destroy();
        reject(new Error('File contains more blocks than specified in the manifest.'));
        return false;
      }

      const compression = blockCompression[manifestHashIndex];
      let blockToHash = fileBlockChunk;
      try {
        if (compression !== COMPRESSION_ALGORITHMS.NONE) {
          blockToHash = compression === COMPRESSION_ALGORITHMS.BROTLI
            ? await limit(() => brotliCompressAsync(fileBlockChunk, { params: { [zlib.constants.BROTLI_PARAM_QUALITY]: 2 } }))
            : await limit(() => deflateAsync(fileBlockChunk));
        }
      } catch (err) {
        fileStream.destroy();
        reject(new Error(`Compression failed for block ${manifestHashIndex + 1}: ${err.message}`));
        return false;
      }

      const computedFileBlockHash = hashBuffer(blockToHash);
      const expectedManifestHash = manifestHashes[manifestHashIndex];

      if (computedFileBlockHash !== expectedManifestHash) {
        fileStream.destroy();
        reject(new Error(`Hash mismatch for file block ${manifestHashIndex + 1}: expected ${expectedManifestHash}, got ${computedFileBlockHash}`));
        return false;
      }

      const storedBlock = await limit(() => readBlock(blockDir, expectedManifestHash));
      const storedBlockHash = hashBuffer(storedBlock);
      if (storedBlockHash !== expectedManifestHash) {
        fileStream.destroy();
        reject(new Error(`Stored block ${manifestHashIndex + 1} is corrupted or does not match manifest: expected ${expectedManifestHash}, got ${storedBlockHash}`));
        return false;
      }

      if (verbose) console.log(`Verified block ${manifestHashIndex + 1}/${manifestHashes.length} (hash: ${expectedManifestHash})`);
      manifestHashIndex++;
      return true;
    };

    fileStream.on('data', async (chunk) => {
      totalBytesReadFromFile += chunk.length;
      currentFileBlockBuffer = Buffer.concat([currentFileBlockBuffer, chunk]);

      while (currentFileBlockBuffer.length >= metadata.blockSize) {
        if (manifestHashIndex >= manifestHashes.length && currentFileBlockBuffer.length > 0) {
          fileStream.destroy();
          reject(new Error('File contains more data than specified by manifest hashes after processing full blocks.'));
          return;
        }
        const blockToProcess = currentFileBlockBuffer.slice(0, metadata.blockSize);
        currentFileBlockBuffer = currentFileBlockBuffer.slice(metadata.blockSize);

        fileStream.pause();
        if (!(await processBlock(blockToProcess))) return;
        fileStream.resume();
      }
    });

    fileStream.on('end', async () => {
      try {
        if (currentFileBlockBuffer.length > 0) {
          if (manifestHashIndex >= manifestHashes.length) {
            reject(new Error('File has trailing data not accounted for in manifest.'));
            return;
          }
          if (!(await processBlock(currentFileBlockBuffer, true))) return;
        }

        if (totalBytesReadFromFile !== metadata.originalSize) {
          reject(new Error(`File size mismatch: manifest indicates ${metadata.originalSize} bytes, but read ${totalBytesReadFromFile} bytes from file.`));
          return;
        }

        if (manifestHashIndex !== manifestHashes.length) {
          reject(new Error(`Block count mismatch: manifest indicates ${manifestHashes.length} blocks, but only processed ${manifestHashIndex} blocks from file.`));
          return;
        }

        resolve(true);
      } catch (err) {
        reject(err);
      }
    });

    fileStream.on('error', (err) => reject(err));
  });
}

/**
 * Writes a logical file from a Buffer.
 * @param {string} filename
 * @param {Buffer} buffer
 * @param {object} [options]
 * @returns {Promise<{ manifestPath: string, blockCount: number, totalSize: number }>}
 */
export async function writeFile(filename, buffer, {
  storageDir = './storage',
  blockSize = 8 * 1024 * 1024, // 8MB
  compress = true,
  verbose = false,
  compressionAlgorithm = COMPRESSION_ALGORITHMS.BROTLI,
  brotliQuality = DEFAULT_BROTLI_QUALITY,
  deflateLevel = DEFAULT_DEFLATE_LEVEL,
} = {}) {
  if (!filename) throw new Error('Filename is required');
  if (!(buffer instanceof Buffer)) throw new Error('Input must be a Buffer');

  const blocksDir = path.join(storageDir, 'blocks');
  const manifestsDir = path.join(storageDir, 'manifests');
  await fs.mkdir(blocksDir, { recursive: true });
  await fs.mkdir(manifestsDir, { recursive: true });

  if (buffer.length === 0) {
    const metadata = {
      originalSize: 0,
      blockSize,
      compress: false,
      compressionAlgorithm: COMPRESSION_ALGORITHMS.NONE,
      originalName: filename,
      createdAt: new Date().toISOString()
    };
    const manifestPath = path.join(manifestsDir, `${filename}.manifest.json`);
    await fs.writeJson(manifestPath, { hashes: [], blockCompression: [], metadata });
    return { manifestPath, blockCount: 0, totalSize: 0 };
  }

  // Fast path for small buffers (<8MB)
  if (buffer.length <= blockSize) {
    let processedBlock = buffer;
    let actualCompression = COMPRESSION_ALGORITHMS.NONE;

    if (compress && shouldCompress(filename, buffer)) {
      actualCompression = compressionAlgorithm;
      try {
        processedBlock = actualCompression === COMPRESSION_ALGORITHMS.BROTLI
          ? await brotliCompressAsync(buffer, { params: { [zlib.constants.BROTLI_PARAM_QUALITY]: 2 } })
          : await deflateAsync(buffer);
        if (processedBlock.length >= buffer.length) {
          processedBlock = buffer;
          actualCompression = COMPRESSION_ALGORITHMS.NONE;
        }
      } catch (err) {
        processedBlock = buffer;
        actualCompression = COMPRESSION_ALGORITHMS.NONE;
      }
    }

    const hash = hashBuffer(processedBlock);
    await writeBlocksBatch(blocksDir, [{ hash, data: processedBlock }], { verbose });
    const metadata = {
      originalSize: buffer.length,
      blockSize: buffer.length, // For single-block files, blockSize in metadata is file size
      compress, // Overall intention to compress
      compressionAlgorithm: actualCompression, // Actual algorithm used for this block
      originalName: filename,
      createdAt: new Date().toISOString(),
      compressionParams: null,
    };
    if (actualCompression === COMPRESSION_ALGORITHMS.BROTLI) {
      metadata.compressionParams = { quality: brotliQuality };
    } else if (actualCompression === COMPRESSION_ALGORITHMS.DEFLATE) {
      metadata.compressionParams = { level: deflateLevel };
    }
    // If actualCompression is NONE, compressionParams remains null

    const manifestPath = path.join(manifestsDir, `${filename}.manifest.json`);
    await fs.writeJson(manifestPath, { hashes: [hash], blockCompression: [actualCompression], metadata });
    return { manifestPath, blockCount: 1, totalSize: buffer.length };
  }

  const hashes = [];
  const blockCompression = [];
  const metadata = {
    originalSize: buffer.length,
    blockSize,
    compress,
    compressionAlgorithm: compress ? compressionAlgorithm : COMPRESSION_ALGORITHMS.NONE,
    originalName: filename,
    createdAt: new Date().toISOString(),
    compressionParams: null, // Will be populated if compression happens
  };

  if (compress) {
    if (metadata.compressionAlgorithm === COMPRESSION_ALGORITHMS.BROTLI) {
      metadata.compressionParams = { quality: brotliQuality };
    } else if (metadata.compressionAlgorithm === COMPRESSION_ALGORITHMS.DEFLATE) {
      metadata.compressionParams = { level: deflateLevel };
    }
  }

  const blocksToWrite = [];

  for (let offset = 0; offset < buffer.length; offset += blockSize) {
    const chunk = buffer.slice(offset, offset + blockSize);
    let blockToStore = chunk;
    let actualCompressionForBlock = COMPRESSION_ALGORITHMS.NONE; // Actual compression for *this specific block*

    if (compress && shouldCompress(filename, chunk)) {
      actualCompressionForBlock = metadata.compressionAlgorithm; // Target algorithm
      let currentCompressionParams = {};
      try {
        if (actualCompressionForBlock === COMPRESSION_ALGORITHMS.BROTLI) {
          currentCompressionParams = { params: { [zlib.constants.BROTLI_PARAM_QUALITY]: brotliQuality } };
          blockToStore = await limit(() => brotliCompressAsync(chunk, currentCompressionParams));
        } else if (actualCompressionForBlock === COMPRESSION_ALGORITHMS.DEFLATE) {
          currentCompressionParams = { level: deflateLevel };
          blockToStore = await limit(() => deflateAsync(chunk, currentCompressionParams));
        } else {
          blockToStore = chunk;
          actualCompressionForBlock = COMPRESSION_ALGORITHMS.NONE;
        }

        if (blockToStore.length >= chunk.length) { // If compression is ineffective
          blockToStore = chunk;
          actualCompressionForBlock = COMPRESSION_ALGORITHMS.NONE;
        }
      } catch (err) {
        if (verbose) console.warn(`Compression failed for a block in writeFile with ${actualCompressionForBlock}, using original. Error: ${err.message}`);
        blockToStore = chunk;
        actualCompressionForBlock = COMPRESSION_ALGORITHMS.NONE;
      }
    } else {
      actualCompressionForBlock = COMPRESSION_ALGORITHMS.NONE;
    }

    const hash = hashBuffer(blockToStore);
    blocksToWrite.push({ hash, data: blockToStore });
    hashes.push(hash);
    blockCompression.push(actualCompressionForBlock); // Store actual compression for the block

    if (blocksToWrite.length >= 8) {
      // Note: blocksToWrite.splice(0) was used before, ensure this is intended.
      // Assuming it means blocksToWrite.splice(0, blocksToWrite.length) or similar for batching.
      // The previous code used blocksToWrite.splice(0, 8) or blocksToWrite.splice(0, blocksToWrite.length)
      // For now, sticking to the logic of previous step which was blocksToWrite.splice(0, 8)
      // This part seems unrelated to the current change, but noting the splice behavior.
      // The refactored storeFile uses blocksToWrite.splice(0, 8) or .splice(0, finalBatch.length)
      // Here, it seems like it should be a fixed batch or all.
      // Let's assume it means a batch of 8.
      await limit(() => writeBlocksBatch(blocksDir, blocksToWrite.splice(0, 8), { verbose }));
    }
  }

  if (blocksToWrite.length > 0) {
    await limit(() => writeBlocksBatch(blocksDir, blocksToWrite, { verbose }));
  }

  const manifestPath = path.join(manifestsDir, `${filename}.manifest.json`);
  await fs.writeJson(manifestPath, { hashes, blockCompression, metadata });

  return { manifestPath, blockCount: hashes.length, totalSize: buffer.length };
}

/**
 * Cleans up unused blocks in the storage directory.
 * @param {string} storageDir
 * @param {object} [options]
 * @returns {Promise<number>}
 */
export async function cleanupBlocks(storageDir = './storage', { verbose = false } = {}) {
  const blockDir = path.join(storageDir, 'blocks');
  const manifestDir = path.join(storageDir, 'manifests');
  if (!(await pathExists(blockDir))) throw new Error(`Block directory not found: ${blockDir}`);
  if (!(await pathExists(manifestDir))) throw new Error(`Manifest directory not found: ${manifestDir}`);

  const blockFiles = await fs.readdir(blockDir);
  const manifests = await fs.readdir(manifestDir);
  const usedHashes = new Set();

  for (const manifest of manifests) {
    const { hashes } = await loadManifest(path.join(manifestDir, manifest));
    hashes.forEach(hash => usedHashes.add(hash));
  }

  let deletedCount = 0;
  for (const blockFile of blockFiles) {
    if (!usedHashes.has(blockFile)) {
      await fs.unlink(path.join(blockDir, blockFile));
      deletedCount++;
      if (verbose) console.log(`Deleted unused block: ${blockFile}`);
    }
  }

  return deletedCount;
}