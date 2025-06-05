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
const limit = pLimit(20); // Increased concurrency for maximum performance

const COMPRESSION_ALGORITHMS = {
  DEFLATE: 'deflate',
  BROTLI: 'brotli',
  NONE: 'none',
};

// Helper function to estimate data entropy for compression decision
function estimateEntropy(buffer) {
  if (buffer.length === 0) return 0;
  const byteCounts = new Uint8Array(256);
  for (const byte of buffer) byteCounts[byte]++;
  let entropy = 0;
  for (const count of byteCounts) {
    if (count > 0) {
      const p = count / buffer.length;
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
    await fs.writeJson(manifestPath, { hashes: [], blockCompression: [], metadata }, { spaces: 2 });
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
    let blockIndex = 0;

    stream.on('data', async (chunk) => {
      if (chunk.length === 0) return;

      stream.pause();
      blockIndex++;
      try {
        let processedBlock = chunk;
        let actualCompression = COMPRESSION_ALGORITHMS.NONE;

        if (compress && chunk.length >= 100) {
          const entropy = estimateEntropy(chunk);
          if (entropy < 6.0) {
            actualCompression = metadata.compressionAlgorithm;
            try {
              if (actualCompression === COMPRESSION_ALGORITHMS.BROTLI) {
                processedBlock = await limit(() => brotliCompressAsync(chunk, {
                  params: { [zlib.constants.BROTLI_PARAM_QUALITY]: 4 } // Faster compression
                }));
              } else {
                processedBlock = await limit(() => deflateAsync(chunk));
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
        await limit(() => writeBlock(blockDir, hash, processedBlock));
        hashes.push(hash);
        blockCompression.push(actualCompression);
        stream.resume();
      } catch (err) {
        stream.destroy();
        reject(new Error(`Failed to process block ${blockIndex}: ${err.message}`));
      }
    });

    stream.on('end', async () => {
      try {
        const manifestPath = path.join(manifestDir, `${fileName}.manifest.json`);
        await fs.writeJson(manifestPath, { hashes, blockCompression, metadata }, { spaces: 2 });
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

  const blocks = [];
  for (let i = 0; i < hashes.length; i++) {
    const hash = hashes[i];
    const compression = blockCompression[i];
    const block = await limit(() => readBlock(blockDir, hash));

    let decompressedBlock = block;
    if (compression !== COMPRESSION_ALGORITHMS.NONE) {
      decompressedBlock = compression === COMPRESSION_ALGORITHMS.BROTLI
        ? await limit(() => brotliDecompressAsync(block))
        : await limit(() => inflateAsync(block));
    }
    blocks.push(decompressedBlock);
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
    await fs.writeJson(manifestPath, { hashes: [], blockCompression: [], metadata }, { spaces: 2 });
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

  for (let offset = 0; offset < buffer.length; offset += blockSize) {
    const chunk = buffer.slice(offset, offset + blockSize);
    let blockToStore = chunk;
    let actualCompression = COMPRESSION_ALGORITHMS.NONE;

    if (compress && chunk.length >= 100) {
      const entropy = estimateEntropy(chunk);
      if (entropy < 6.0) {
        actualCompression = metadata.compressionAlgorithm;
        try {
          blockToStore = actualCompression === COMPRESSION_ALGORITHMS.BROTLI
            ? await limit(() => brotliCompressAsync(chunk, {
                params: { [zlib.constants.BROTLI_PARAM_QUALITY]: 4 }
              }))
            : await limit(() => deflateAsync(chunk));
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
    await limit(() => writeBlock(blocksDir, hash, blockToStore));
    hashes.push(hash);
    blockCompression.push(actualCompression);
  }

  const manifestPath = path.join(manifestsDir, `${filename}.manifest.json`);
  await fs.writeJson(manifestPath, { hashes, blockCompression, metadata }, { spaces: 2 });

  return { manifestPath, blockCount: hashes.length, totalSize: buffer.length };
}