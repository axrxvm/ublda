import path from 'path';
import fs from 'fs-extra';
import zlib from 'zlib';
import { promisify } from 'util';
import pLimit from 'p-limit';
import { splitFile, writeFileFromBlocks, createReadStreamFromBlocks } from './file.js';
import { hashBuffer } from './hasher.js';
import { writeBlock, readBlock } from './storage.js';
import { saveManifest, loadManifest } from './manifest.js';

const deflateAsync = promisify(zlib.deflate);
const inflateAsync = promisify(zlib.inflate);
const brotliCompressAsync = promisify(zlib.brotliCompress);
const brotliDecompressAsync = promisify(zlib.brotliDecompress);
const limit = pLimit(3); // Reduced from 10 to lower memory usage

const COMPRESSION_ALGORITHMS = {
  DEFLATE: 'deflate',
  BROTLI: 'brotli',
  NONE: 'none',
};

// Helper function to check file/directory existence
async function pathExists(path) {
  try {
    await fs.access(path);
    return true;
  } catch {
    return false;
  }
}

/**
 * Store a file as blocks + manifest.
 * @param {string} filePath
 * @param {object} [options]
 * @returns {Promise<{ manifestPath: string, blockCount: number }>}
 */
export async function storeFile(filePath, {
  storageDir = './storage',
  blockSize = 4096,
  compress = false,
  verbose = false,
  compressionAlgorithm = COMPRESSION_ALGORITHMS.DEFLATE, // New option
} = {}) {
  if (!(await pathExists(filePath))) throw new Error(`File not found: ${filePath}`);
  if (blockSize <= 0) throw new Error('Block size must be positive');

  const fileName = path.basename(filePath);
  const blockDir = path.join(storageDir, 'blocks');
  const manifestDir = path.join(storageDir, 'manifests');
  await fs.ensureDir(blockDir);
  await fs.ensureDir(manifestDir);

  const hashes = [];
  let actualCompressionAlgo = COMPRESSION_ALGORITHMS.NONE;
  if (compress) {
    if (Object.values(COMPRESSION_ALGORITHMS).includes(compressionAlgorithm) && compressionAlgorithm !== COMPRESSION_ALGORITHMS.NONE) {
      actualCompressionAlgo = compressionAlgorithm;
    } else {
      actualCompressionAlgo = COMPRESSION_ALGORITHMS.DEFLATE; // Default if invalid or 'none' chosen with compress:true
      if (verbose && compressionAlgorithm !== COMPRESSION_ALGORITHMS.DEFLATE) {
        console.warn(`Invalid compression algorithm "${compressionAlgorithm}", defaulting to "${COMPRESSION_ALGORITHMS.DEFLATE}".`);
      }
    }
  }

  const metadata = {
    originalSize: (await fs.stat(filePath)).size,
    blockSize, // Store block size
    compress, // Keep this for quick check and backward compatibility
    compressionAlgorithm: actualCompressionAlgo, // Store the actual algorithm used
    originalName: fileName,
    createdAt: new Date().toISOString()
  };

  return new Promise((resolve, reject) => {
    const stream = fs.createReadStream(filePath);
    let currentChunk = Buffer.alloc(0);
    let blockIndex = 0;
    const processingPromises = []; // Array to hold all block processing promises

    stream.on('data', (data) => {
      currentChunk = Buffer.concat([currentChunk, data]);

      while (currentChunk.length >= blockSize) {
        const chunkToProcess = currentChunk.slice(0, blockSize);
        currentChunk = currentChunk.slice(blockSize);
        blockIndex++;

        const currentBlockIndex = blockIndex; // Capture blockIndex for this iteration
        processingPromises.push((async () => {
          try {
            let processedBlock = chunkToProcess;
            if (metadata.compress) {
              if (metadata.compressionAlgorithm === COMPRESSION_ALGORITHMS.BROTLI) {
                // if (verbose) console.log(`[STOREFILE-BROTLI] Compressing block ${currentBlockIndex}. Original chunk length: ${chunkToProcess.length}, hash: ${hashBuffer(chunkToProcess)}`);
                processedBlock = await brotliCompressAsync(chunkToProcess);
                // if (verbose) console.log(`[STOREFILE-BROTLI] Compressed block ${currentBlockIndex}. Compressed length: ${processedBlock.length}, hash: ${hashBuffer(processedBlock)}`);
              } else { // Default to deflate
                processedBlock = await deflateAsync(chunkToProcess); // Use chunkToProcess here
              }
            }
            const hash = hashBuffer(processedBlock);
            // if (metadata.compressionAlgorithm === COMPRESSION_ALGORITHMS.BROTLI && verbose) {
            //   console.log(`[STOREFILE-BROTLI] Storing block ${currentBlockIndex} with final hash: ${hash}`);
            // }
            await limit(() => writeBlock(blockDir, hash, processedBlock, { verbose }));
            // hashes.push(hash); // DO NOT PUSH HERE
            // if (verbose) console.log(`Stored block ${currentBlockIndex} (hash: ${hash})`);
            return { index: currentBlockIndex, hash }; // Return index and hash
          } catch (err) {
            stream.destroy();
            if (!stream.destroyed && typeof reject === 'function') reject(err);
            throw err;
          }
        })());
      }
    });

    stream.on('end', async () => {
      try {
        if (currentChunk.length > 0) {
          blockIndex++;
          const finalBlockIndex = blockIndex;
          processingPromises.push((async () => {
            try {
              let processedBlock = currentChunk;
              if (metadata.compress) {
                if (metadata.compressionAlgorithm === COMPRESSION_ALGORITHMS.BROTLI) {
                // if (verbose) console.log(`[STOREFILE-BROTLI] Compressing FINAL block ${finalBlockIndex}. Original chunk length: ${currentChunk.length}, hash: ${hashBuffer(currentChunk)}`);
                  processedBlock = await brotliCompressAsync(currentChunk);
                // if (verbose) console.log(`[STOREFILE-BROTLI] Compressed FINAL block ${finalBlockIndex}. Compressed length: ${processedBlock.length}, hash: ${hashBuffer(processedBlock)}`);
                } else {
                  processedBlock = await deflateAsync(currentChunk);
                }
              }
              const hash = hashBuffer(processedBlock);
            // if (metadata.compressionAlgorithm === COMPRESSION_ALGORITHMS.BROTLI && verbose) {
            //   console.log(`[STOREFILE-BROTLI] Storing FINAL block ${finalBlockIndex} with final hash: ${hash}`);
            // }
              await limit(() => writeBlock(blockDir, hash, processedBlock, { verbose }));
              // hashes.push(hash); // DO NOT PUSH HERE
            // if (verbose) console.log(`Stored block ${finalBlockIndex} (final) (hash: ${hash})`);
              return { index: finalBlockIndex, hash }; // Return index and hash
            } catch (err) {
                stream.destroy();
                if (!stream.destroyed && typeof reject === 'function') reject(err);
                throw err;
            }
          })());
        }

        const CHUNK_RESULTS_UNORDERED = await Promise.all(processingPromises);

        // Sort the results by original block index to ensure correct hash order
        CHUNK_RESULTS_UNORDERED.sort((a, b) => a.index - b.index);
        const orderedHashes = CHUNK_RESULTS_UNORDERED.map(item => item.hash);

        const manifestPath = await saveManifest(manifestDir, fileName, { hashes: orderedHashes, metadata });
        resolve({ manifestPath, blockCount: orderedHashes.length });
      } catch (err) {
        // Ensure the stream is destroyed and reject the main promise
        if(!stream.destroyed) stream.destroy();
        reject(err);
      }
    });

    stream.on('error', (err) => {
      // If an error occurs on the stream, reject the main promise.
      // Ensure any pending promises in processingPromises are also handled or ignored.
      // (Promise.all will reject if any of its promises reject)
      reject(err);
    });
  });
}

// Adjusted restoreFile structure to handle empty file promise correctly
export async function restoreFile(manifestPath, outputPath, {
  storageDir = './storage',
  verifyHashes = false,
  verbose = false,
} = {}) {
  if (!(await pathExists(manifestPath))) throw new Error(`Manifest file not found: ${manifestPath}`);
  const blockDir = path.join(storageDir, 'blocks');
  if (!(await pathExists(blockDir))) throw new Error(`Block directory not found: ${blockDir}`);

  const { hashes, metadata } = await loadManifest(manifestPath);

  if (!Array.isArray(hashes)) { // Basic validation
      throw new Error('Invalid manifest: hashes is not an array.');
  }

  if (hashes.length === 0 && metadata.originalSize === 0) {
    // Legitimate empty file
    if (verbose) console.log(`Restoring empty file: ${outputPath}`);
    await fs.writeFile(outputPath, '');
    return Promise.resolve(); // Explicitly return a resolved promise
  }

  if (hashes.length === 0 && metadata.originalSize !== 0) {
    // Invalid manifest: non-empty file but no hashes
    return Promise.reject(new Error('Manifest is invalid: non-empty file indicated by originalSize but no hashes found.'));
  }

  if (hashes.length > 0 && metadata.originalSize === 0) {
      // Invalid manifest: empty file but hashes found
      return Promise.reject(new Error('Manifest is invalid: empty file indicated by originalSize but hashes are present.'));
  }


  return new Promise(async (resolve, reject) => {
    const outputStream = fs.createWriteStream(outputPath);

    outputStream.on('finish', () => resolve());
    outputStream.on('error', (err) => reject(err));

    try {
      for (let i = 0; i < hashes.length; i++) {
        const hash = hashes[i];
        const block = await limit(() => readBlock(blockDir, hash));

        if (verifyHashes) {
          // Verify hash of the raw block (potentially compressed)
          const computedHash = hashBuffer(block);
          if (computedHash !== hash) {
            const errMsg = `Hash mismatch for block ${i + 1}: expected ${hash}, got ${computedHash}`;
            outputStream.destroy(new Error(errMsg)); // Close stream with error
            reject(new Error(errMsg));
            return;
          }
        }

        let dataToWrite = block;
        if (metadata.compress) {
          if (metadata.compressionAlgorithm === COMPRESSION_ALGORITHMS.BROTLI) {
            // if (verbose) console.log(`[RESTOREFILE-BROTLI] Decompressing block ${i+1} (hash from manifest: ${hash}). Raw block length: ${block.length}, raw block hash: ${hashBuffer(block)}`);
            dataToWrite = await brotliDecompressAsync(block);
            // if (verbose) console.log(`[RESTOREFILE-BROTLI] Decompressed block ${i+1}. Decompressed length: ${dataToWrite.length}, hash: ${hashBuffer(dataToWrite)}`);
          } else { // Handles deflate and undefined algorithm for backward compatibility
            dataToWrite = await inflateAsync(block);
          }
        }

        if (!outputStream.write(dataToWrite)) {
          // Handle backpressure if write() returns false
          await new Promise(drainResolve => outputStream.once('drain', drainResolve));
        }

        // if (verbose) console.log(`Restored block ${i + 1}/${hashes.length} (hash: ${hash})`);
      }
      outputStream.end();
    } catch (err) {
      outputStream.destroy(err); // Ensure stream is destroyed on error
      reject(err);
    }
  });
}

/**
 * Reads logical file from manifest and blocks.
 * @param {string} manifestPath
 * @param {object} [options]
 * @returns {Promise<Buffer|Readable>}
 */
export async function readFile(manifestPath, {
  storageDir = './storage',
  verifyHashes = false,
  verbose = false,
  stream = false,
} = {}) {
  if (!manifestPath.endsWith('.json')) throw new Error('Manifest path must point to a .json file');
  if (!(await pathExists(manifestPath))) throw new Error(`Manifest file not found: ${manifestPath}`);

  const blockDir = path.join(storageDir, 'blocks');
  if (!(await pathExists(blockDir))) throw new Error(`Block directory not found: ${blockDir}`);

  const { hashes, metadata } = await loadManifest(manifestPath);
  if (!Array.isArray(hashes) || hashes.length === 0) throw new Error('Manifest is empty or invalid');

  if (stream) {
    // Pass compressionAlgorithm to the stream creator
    return createReadStreamFromBlocks(blockDir, hashes, {
      compress: metadata.compress,
      compressionAlgorithm: metadata.compressionAlgorithm, // Pass this along
      verifyHashes,
      verbose
    });
  }

  const blocks = [];
  for (let i = 0; i < hashes.length; i++) {
    const hash = hashes[i];
    const blockData = await limit(() => readBlock(blockDir, hash)); // Renamed to avoid conflict

    if (verifyHashes) {
      const computedHash = hashBuffer(blockData); // Verify hash of the raw block
      if (computedHash !== hash) throw new Error(`Hash mismatch for block ${i + 1}: expected ${hash}, got ${computedHash}`);
    }

    let decompressedBlock = blockData;
    if (metadata.compress) {
      if (metadata.compressionAlgorithm === COMPRESSION_ALGORITHMS.BROTLI) {
        decompressedBlock = await brotliDecompressAsync(blockData);
      } else { // Handles deflate and undefined algorithm
        decompressedBlock = await inflateAsync(blockData);
      }
    }
    blocks.push(decompressedBlock);
    if (verbose) console.log(`Read block ${i + 1}/${hashes.length} (hash: ${hash})`);
  }

  return Buffer.concat(blocks);
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
  blockSize = 1024 * 1024,
  compress = false,
  verbose = false,
} = {}) {
  if (!filename) throw new Error('Filename is required');
  if (!(buffer instanceof Buffer)) throw new Error('Input must be a Buffer');

  const blocksDir = path.join(storageDir, 'blocks');
  const manifestsDir = path.join(storageDir, 'manifests');
  await fs.ensureDir(blocksDir);
  await fs.ensureDir(manifestsDir);

  const hashes = [];
  // For writeFile, if compression is true, it uses deflate. No Brotli option here for simplicity.
  // Metadata will reflect this by not storing compressionAlgorithm or defaulting to deflate.
  const metadata = {
    originalSize: buffer.length,
    blockSize,
    compress,
    compressionAlgorithm: compress ? COMPRESSION_ALGORITHMS.DEFLATE : COMPRESSION_ALGORITHMS.NONE,
    originalName: filename,
    createdAt: new Date().toISOString()
  };

  for (let offset = 0; offset < buffer.length; offset += blockSize) {
    const chunk = buffer.slice(offset, offset + blockSize);
    let blockToStore = chunk; // Renamed to avoid confusion
    if (compress) blockToStore = await deflateAsync(chunk); // writeFile defaults to deflate
    const hash = hashBuffer(blockToStore);
    await limit(() => writeBlock(blocksDir, hash, blockToStore, { verbose }));
    hashes.push(hash);
    if (verbose) console.log(`Wrote block ${hashes.length}/${Math.ceil(buffer.length / blockSize)} (hash: ${hash})`);
  }

  const manifestPath = path.join(manifestsDir, `${filename}.manifest.json`);
  await fs.writeJson(manifestPath, { hashes, metadata }, { spaces: 2 });

  for (const hash of hashes) {
    const blockPath = path.join(blocksDir, hash);
    if (!(await pathExists(blockPath))) throw new Error(`Block missing after write: ${blockPath}`);
  }

  return { manifestPath, blockCount: hashes.length, totalSize: buffer.length };
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
  if (!(await pathExists(filePath))) throw new Error(`File not found: ${filePath}`);
  if (!(await pathExists(manifestPath))) throw new Error(`Manifest file not found: ${manifestPath}`);

  const { hashes: manifestHashes, metadata } = await loadManifest(manifestPath);
  if (!metadata.blockSize) throw new Error('Manifest missing blockSize metadata');
  if (!Array.isArray(manifestHashes) || manifestHashes.length === 0 && metadata.originalSize > 0) {
    throw new Error('Manifest contains no hashes for a non-empty file.');
  }
   if (metadata.originalSize === 0 && manifestHashes.length > 0) {
    throw new Error('Manifest contains hashes for an empty file.');
  }
   if (metadata.originalSize === 0 && manifestHashes.length === 0) {
    if (verbose) console.log('File and manifest correctly represent an empty file.');
    // Check if the actual file on disk is also empty
    const actualFileStat = await fs.stat(filePath);
    if (actualFileStat.size !== 0) {
      throw new Error(`Manifest indicates empty file, but file on disk has size ${actualFileStat.size}.`);
    }
    return true; // Correctly verified an empty file
  }


  const blockDir = path.join(storageDir, 'blocks');

  return new Promise((resolve, reject) => {
    const fileStream = fs.createReadStream(filePath);
    let currentFileBlockBuffer = Buffer.alloc(0);
    let manifestHashIndex = 0;
    let totalBytesReadFromFile = 0;

    const processBlock = async (fileBlockChunk, isLastBlock = false) => {
      try {
        if (manifestHashIndex >= manifestHashes.length) {
          // This means we have more data from fileStream than indicated by manifest hashes
          fileStream.destroy(); // Stop reading
          reject(new Error('File contains more blocks than specified in the manifest.'));
          return false; // Indicate processing should stop
        }

        let blockToHash = fileBlockChunk;
        if (metadata.compress) {
          if (metadata.compressionAlgorithm === COMPRESSION_ALGORITHMS.BROTLI) {
            blockToHash = await brotliCompressAsync(fileBlockChunk);
          } else { // Default to deflate
            blockToHash = await deflateAsync(fileBlockChunk);
          }
        }

        const computedFileBlockHash = hashBuffer(blockToHash);
        const expectedManifestHash = manifestHashes[manifestHashIndex];

        // Primary verification: Compare hash of (compressed) file block with manifest hash
        if (computedFileBlockHash !== expectedManifestHash) {
          fileStream.destroy();
          reject(new Error(`Hash mismatch for file block ${manifestHashIndex + 1}: expected ${expectedManifestHash}, got ${computedFileBlockHash}`));
          return false;
        }

        // Secondary verification: Ensure block in storage matches manifest hash
        // This is somewhat redundant if storeFile is trusted, but good for full verification.
        const storedBlock = await limit(() => readBlock(blockDir, expectedManifestHash));
        const storedBlockHash = hashBuffer(storedBlock);
        if (storedBlockHash !== expectedManifestHash) {
          fileStream.destroy();
          reject(new Error(`Stored block ${manifestHashIndex + 1} is corrupted or does not match manifest: expected ${expectedManifestHash}, got ${storedBlockHash}`));
          return false;
        }

        if (verbose) console.log(`Verified block ${manifestHashIndex + 1}/${manifestHashes.length} (hash: ${expectedManifestHash})`);
        manifestHashIndex++;
        return true; // Indicate successful processing of this block
      } catch (err) {
        fileStream.destroy();
        reject(err);
        return false;
      }
    };

    fileStream.on('data', async (chunk) => {
      totalBytesReadFromFile += chunk.length;
      currentFileBlockBuffer = Buffer.concat([currentFileBlockBuffer, chunk]);

      while (currentFileBlockBuffer.length >= metadata.blockSize) {
        if (manifestHashIndex >= manifestHashes.length && currentFileBlockBuffer.length > 0) {
           // If we've processed all manifest hashes but still have full blocks of data
           fileStream.destroy();
           reject(new Error('File contains more data than specified by manifest hashes after processing full blocks.'));
           return;
        }
        const blockToProcess = currentFileBlockBuffer.slice(0, metadata.blockSize);
        currentFileBlockBuffer = currentFileBlockBuffer.slice(metadata.blockSize);

        fileStream.pause(); // Pause stream while async processing happens
        const success = await processBlock(blockToProcess);
        if (!success) return; // Error already handled by processBlock
        fileStream.resume(); // Resume stream
      }
    });

    fileStream.on('end', async () => {
      try {
        if (currentFileBlockBuffer.length > 0) {
          if (manifestHashIndex >= manifestHashes.length) {
             // Leftover data in buffer but no more manifest hashes
            reject(new Error('File has trailing data not accounted for in manifest.'));
            return;
          }
          const success = await processBlock(currentFileBlockBuffer, true);
          if (!success) return; // Error handled
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

    fileStream.on('error', (err) => {
      reject(err);
    });
  });
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