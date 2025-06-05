import path from 'path';
import fs from 'fs-extra';
import zlib from 'zlib';
import { promisify } from 'util';
import pLimit from 'p-limit';
import { splitFile, writeFileFromBlocks, createReadStreamFromBlocks } from './file.js';
import { hashBuffer } from './hasher.js';
import { writeBlock, readBlock } from './storage.js';
import { saveManifest, loadManifest } from './manifest.js';
import os from 'os';

// Dynamic concurrency based on CPU cores
const limit = pLimit(Math.max(2, os.cpus().length));

const deflateAsync = promisify(zlib.deflate);
const inflateAsync = promisify(zlib.inflate);
const brotliCompressAsync = promisify(zlib.brotliCompress);
const brotliDecompressAsync = promisify(zlib.brotliDecompress);

const COMPRESSION_ALGORITHMS = {
  DEFLATE: 'deflate',
  BROTLI: 'brotli',
  NONE: 'none',
};

// Optimized entropy estimation with sampling
function estimateEntropy(buffer) {
  if (buffer.length < 100) return 8; // Skip for small buffers
  const sampleSize = Math.min(1024, buffer.length);
  const sample = buffer.slice(0, sampleSize);
  const byteCounts = new Uint8Array(256);
  for (const byte of sample) byteCounts[byte]++;
  let entropy = 0;
  for (const count of byteCounts) {
    if (count > 0) {
      const p = count / sampleSize;
      entropy -= p * Math.log2(p);
    }
  }
  return entropy;
}

// Helper function to validate compressed data
async function validateCompressedBlock(block, algorithm) {
  try {
    if (algorithm === COMPRESSION_ALGORITHMS.BROTLI) {
      await brotliDecompressAsync(block);
    } else if (algorithm === COMPRESSION_ALGORITHMS.DEFLATE) {
      await inflateAsync(block);
    }
    return true;
  } catch {
    return false;
  }
}

// Helper function to check file/directory existence
async function pathExists(path) {
  return fs.access(path).then(() => true).catch(() => false);
}

/**
 * Store a file as blocks + manifest.
 * @param {string} filePath
 * @param {object} [options]
 * @returns {Promise<{ manifestPath: string, blockCount: number }>}
 */
export async function storeFile(filePath, {
  storageDir = './storage',
  blockSize = 1048576,
  compress = true,
  verbose = false,
  compressionAlgorithm = COMPRESSION_ALGORITHMS.BROTLI,
} = {}) {
  if (!(await pathExists(filePath))) throw new Error(`File not found: ${filePath}`);
  if (blockSize <= 0) throw new Error('Block size must be positive');

  const fileName = path.basename(filePath);
  const blockDir = path.join(storageDir, 'blocks');
  const manifestDir = path.join(storageDir, 'manifests');
  await fs.ensureDir(blockDir);
  await fs.ensureDir(manifestDir);

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

  const metadata = {
    originalSize: stats.size,
    blockSize,
    compress,
    compressionAlgorithm: compress ? compressionAlgorithm : COMPRESSION_ALGORITHMS.NONE,
    originalName: fileName,
    createdAt: new Date().toISOString()
  };

  return new Promise((resolve, reject) => {
    const stream = fs.createReadStream(filePath, { highWaterMark: blockSize });
    const hashes = [];
    const blockCompression = [];
    const blockWritePromises = [];
    let blockIndex = 0;

    stream.on('data', async (chunk) => {
      if (chunk.length === 0) return;

      stream.pause();
      blockIndex++;
      blockWritePromises.push(limit(async () => {
        let processedBlock = chunk;
        let actualCompression = COMPRESSION_ALGORITHMS.NONE;

        if (compress && chunk.length >= 1024) { // Skip compression for small blocks
          const entropy = estimateEntropy(chunk);
          if (entropy < 6.0) {
            actualCompression = metadata.compressionAlgorithm;
            try {
              if (actualCompression === COMPRESSION_ALGORITHMS.BROTLI) {
                processedBlock = await brotliCompressAsync(chunk, {
                  params: { [zlib.constants.BROTLI_PARAM_QUALITY]: 2 }
                });
              } else {
                processedBlock = await deflateAsync(chunk);
              }
              if (!(await validateCompressedBlock(processedBlock, actualCompression))) {
                processedBlock = chunk;
                actualCompression = COMPRESSION_ALGORITHMS.NONE;
              } else if (processedBlock.length >= chunk.length) {
                processedBlock = chunk;
                actualCompression = COMPRESSION_ALGORITHMS.NONE;
              }
            } catch {
              processedBlock = chunk;
              actualCompression = COMPRESSION_ALGORITHMS.NONE;
            }
          }
        }

        const hash = hashBuffer(processedBlock);
        await writeBlock(blockDir, hash, processedBlock, { verbose });
        return { hash, compression: actualCompression };
      }));

      stream.resume();
    });

    stream.on('end', async () => {
      try {
        const results = await Promise.all(blockWritePromises);
        results.forEach(({ hash, compression }) => {
          hashes.push(hash);
          blockCompression.push(compression);
        });
        const manifestPath = path.join(manifestDir, `${fileName}.manifest.json`);
        await fs.writeJson(manifestPath, { hashes, blockCompression, metadata });
        resolve({ manifestPath, blockCount: hashes.length });
      } catch (err) {
        stream.destroy();
        reject(new Error(`Failed to save manifest: ${err.message}`));
      }
    });

    stream.on('error', (err) => {
      reject(new Error(`Stream error: ${err.message}`));
    });
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
  if (!(await pathExists(manifestPath))) throw new Error(`Manifest file not found: ${manifestPath}`);
  const blockDir = path.join(storageDir, 'blocks');
  if (!(await pathExists(blockDir))) throw new Error(`Block directory not found: ${blockDir}`);

  const { hashes, blockCompression, metadata } = await loadManifest(manifestPath);
  if (!Array.isArray(hashes) || !Array.isArray(blockCompression) || hashes.length !== blockCompression.length) {
    throw new Error('Invalid manifest: hashes or blockCompression is missing or inconsistent.');
  }

  if (hashes.length === 0 && metadata.originalSize === 0) {
    await fs.writeFile(outputPath, '');
    return;
  }

  if (hashes.length === 0 || metadata.originalSize === 0) {
    throw new Error('Manifest is invalid: inconsistent hashes and originalSize.');
  }

  const outputStream = fs.createWriteStream(outputPath);
  return new Promise((resolve, reject) => {
    outputStream.on('finish', () => resolve());
    outputStream.on('error', (err) => reject(err));

    (async () => {
      try {
        for (let i = 0; i < hashes.length; i++) {
          const hash = hashes[i];
          const compression = blockCompression[i];
          const block = await limit(() => readBlock(blockDir, hash));

          let dataToWrite = block;
          if (compression !== COMPRESSION_ALGORITHMS.NONE) {
            dataToWrite = compression === COMPRESSION_ALGORITHMS.BROTLI
              ? await limit(() => brotliDecompressAsync(block))
              : await limit(() => inflateAsync(block));
          }

          if (!outputStream.write(dataToWrite)) {
            await new Promise(resolve => outputStream.once('drain', resolve));
          }
        }
        outputStream.end();
      } catch (err) {
        outputStream.destroy(err);
        reject(err);
      }
    })();
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
  verbose = false,
  stream = false,
} = {}) {
  if (!manifestPath.endsWith('.json')) throw new Error('Manifest path must point to a .json file');
  if (!(await pathExists(manifestPath))) throw new Error(`Manifest file not found: ${manifestPath}`);

  const blockDir = path.join(storageDir, 'blocks');
  if (!(await pathExists(blockDir))) throw new Error(`Block directory not found: ${blockDir}`);

  const { hashes, blockCompression, metadata } = await loadManifest(manifestPath);
  if (!Array.isArray(hashes) || !Array.isArray(blockCompression) || hashes.length !== blockCompression.length) {
    throw new Error('Invalid manifest: hashes or blockCompression is missing or inconsistent.');
  }

  if (stream) {
    return createReadStreamFromBlocks(blockDir, hashes, {
      compress: metadata.compress,
      compressionAlgorithm: metadata.compressionAlgorithm,
      verbose,
      blockCompression
    });
  }

  const blockPromises = hashes.map(async (hash, i) => {
    const compression = blockCompression[i];
    const block = await limit(() => readBlock(blockDir, hash));
    return compression !== COMPRESSION_ALGORITHMS.NONE
      ? compression === COMPRESSION_ALGORITHMS.BROTLI
        ? await limit(() => brotliDecompressAsync(block))
        : await limit(() => inflateAsync(block))
      : block;
  });

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
  if (!(await pathExists(filePath))) throw new Error(`File not found: ${filePath}`);
  if (!(await pathExists(manifestPath))) throw new Error(`Manifest file not found: ${manifestPath}`);

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
    if (actualFileStat.size !== 0) {
      throw new Error(`Manifest indicates empty file, but file on disk has size ${actualFileStat.size}.`);
    }
    if (verbose) console.log('File and manifest correctly represent an empty file.');
    return true;
  }

  const blockDir = path.join(storageDir, 'blocks');

  return new Promise((resolve, reject) => {
    const fileStream = fs.createReadStream(filePath, { highWaterMark: metadata.blockSize });
    let currentFileBlockBuffer = Buffer.alloc(0);
    let manifestHashIndex = 0;
    let totalBytesReadFromFile = 0;

    const processBlock = async (fileBlockChunk, isLastBlock = false) => {
      if (manifestHashIndex >= manifestHashes.length) {
        fileStream.destroy();
        reject(new Error('File contains more blocks than specified in the manifest.'));
        return false;
      }

      let blockToHash = fileBlockChunk;
      const compression = blockCompression[manifestHashIndex];
      if (compression !== COMPRESSION_ALGORITHMS.NONE) {
        blockToHash = compression === COMPRESSION_ALGORITHMS.BROTLI
          ? await limit(() => brotliCompressAsync(fileBlockChunk, {
              params: { [zlib.constants.BROTLI_PARAM_QUALITY]: 2 }
            }))
          : await limit(() => deflateAsync(fileBlockChunk));
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
        const success = await processBlock(blockToProcess);
        if (!success) return;
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
          const success = await processBlock(currentFileBlockBuffer, true);
          if (!success) return;
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
 * Writes a logical file from a Buffer.
 * @param {string} filename
 * @param {Buffer} buffer
 * @param {object} [options]
 * @returns {Promise<{ manifestPath: string, blockCount: number, totalSize: number }>}
 */
export async function writeFile(filename, buffer, {
  storageDir = './storage',
  blockSize = 1048576,
  compress = true,
  verbose = false,
} = {}) {
  if (!filename) throw new Error('Filename is required');
  if (!(buffer instanceof Buffer)) throw new Error('Input must be a Buffer');

  const blocksDir = path.join(storageDir, 'blocks');
  const manifestsDir = path.join(storageDir, 'manifests');
  await fs.ensureDir(blocksDir);
  await fs.ensureDir(manifestsDir);

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

  const hashes = [];
  const blockCompression = [];
  const metadata = {
    originalSize: buffer.length,
    blockSize,
    compress,
    compressionAlgorithm: compress ? COMPRESSION_ALGORITHMS.BROTLI : COMPRESSION_ALGORITHMS.NONE,
    originalName: filename,
    createdAt: new Date().toISOString()
  };

  const blockPromises = [];
  for (let offset = 0; offset < buffer.length; offset += blockSize) {
    const chunk = buffer.slice(offset, offset + blockSize);
    blockPromises.push(limit(async () => {
      let blockToStore = chunk;
      let actualCompression = COMPRESSION_ALGORITHMS.NONE;

      if (compress && chunk.length >= 1024) { // Skip compression for small blocks
        const entropy = estimateEntropy(chunk);
        if (entropy < 6.0) {
          actualCompression = metadata.compressionAlgorithm;
          try {
            blockToStore = actualCompression === COMPRESSION_ALGORITHMS.BROTLI
              ? await brotliCompressAsync(chunk, {
                  params: { [zlib.constants.BROTLI_PARAM_QUALITY]: 2 }
                })
              : await deflateAsync(chunk);
            if (!(await validateCompressedBlock(blockToStore, actualCompression))) {
              blockToStore = chunk;
              actualCompression = COMPRESSION_ALGORITHMS.NONE;
            } else if (blockToStore.length >= chunk.length) {
              blockToStore = chunk;
              actualCompression = COMPRESSION_ALGORITHMS.NONE;
            }
          } catch {
            blockToStore = chunk;
            actualCompression = COMPRESSION_ALGORITHMS.NONE;
          }
        }
      }

      const hash = hashBuffer(blockToStore);
      await writeBlock(blocksDir, hash, blockToStore, { verbose });
      return { hash, compression: actualCompression };
    }));
  }

  const results = await Promise.all(blockPromises);
  results.forEach(({ hash, compression }) => {
    hashes.push(hash);
    blockCompression.push(compression);
  });

  const manifestPath = path.join(manifestsDir, `${filename}.manifest.json`);
  await fs.writeJson(manifestPath, { hashes, blockCompression, metadata });

  return { manifestPath, blockCount: hashes.length, totalSize: buffer.length };
}