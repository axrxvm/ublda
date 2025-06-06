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
    createdAt: new Date().toISOString()
  };

  const stream = fs.createReadStream(filePath, { highWaterMark: blockSize });
  const hashes = [];
  const blockCompression = [];
  const blocksToWrite = [];
  let blockIndex = 0;

  return new Promise((resolve, reject) => {
    stream.on('data', async (chunk) => {
      if (chunk.length === 0) return;

      stream.pause();
      blockIndex++;
      let processedBlock = chunk;
      let actualCompression = COMPRESSION_ALGORITHMS.NONE;

      if (compress && shouldCompress(fileName, chunk)) {
        actualCompression = metadata.compressionAlgorithm;
        try {
          processedBlock = actualCompression === COMPRESSION_ALGORITHMS.BROTLI
            ? await limit(() => brotliCompressAsync(chunk, { params: { [zlib.constants.BROTLI_PARAM_QUALITY]: 2 } }))
            : await limit(() => deflateAsync(chunk));
          if (processedBlock.length >= chunk.length) {
            processedBlock = chunk;
            actualCompression = COMPRESSION_ALGORITHMS.NONE;
          }
        } catch {
          processedBlock = chunk;
          actualCompression = COMPRESSION_ALGORITHMS.NONE;
        }
      }

      const hash = hashBuffer(processedBlock);
      blocksToWrite.push({ hash, data: processedBlock });
      hashes.push(hash);
      blockCompression.push(actualCompression);

      if (blocksToWrite.length >= 8) { // Batch writes
        await limit(() => writeBlocksBatch(blockDir, blocksToWrite.splice(0), { verbose }));
      }

      stream.resume();
    });

    stream.on('end', async () => {
      if (blocksToWrite.length > 0) {
        await limit(() => writeBlocksBatch(blockDir, blocksToWrite, { verbose }));
      }
      const manifestPath = path.join(manifestDir, `${fileName}.manifest.json`);
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

  if (hashes.length === 0 || metadata.originalSize === 0) {
    throw new Error('Manifest is invalid: inconsistent hashes and originalSize.');
  }

  const outputStream = fs.createWriteStream(outputPath);
  await pipeline(
    createReadStreamFromBlocks(blockDir, hashes, { blockCompression, verbose, compress: metadata.compress, compressionAlgorithm: metadata.compressionAlgorithm }),
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

  if (stream) {
    return createReadStreamFromBlocks(blockDir, hashes, {
      compress: metadata.compress,
      compressionAlgorithm: metadata.compressionAlgorithm,
      verbose,
      blockCompression
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
      blockSize: buffer.length,
      compress,
      compressionAlgorithm: actualCompression,
      originalName: filename,
      createdAt: new Date().toISOString()
    };
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
    createdAt: new Date().toISOString()
  };
  const blocksToWrite = [];

  for (let offset = 0; offset < buffer.length; offset += blockSize) {
    const chunk = buffer.slice(offset, offset + blockSize);
    let blockToStore = chunk;
    let actualCompression = COMPRESSION_ALGORITHMS.NONE;

    if (compress && shouldCompress(filename, chunk)) {
      actualCompression = metadata.compressionAlgorithm;
      try {
        blockToStore = actualCompression === COMPRESSION_ALGORITHMS.BROTLI
          ? await limit(() => brotliCompressAsync(chunk, { params: { [zlib.constants.BROTLI_PARAM_QUALITY]: 2 } }))
          : await limit(() => deflateAsync(chunk));
        if (blockToStore.length >= chunk.length) {
          blockToStore = chunk;
          actualCompression = COMPRESSION_ALGORITHMS.NONE;
        }
      } catch (err) {
        blockToStore = chunk;
        actualCompression = COMPRESSION_ALGORITHMS.NONE;
      }
    }

    const hash = hashBuffer(blockToStore);
    blocksToWrite.push({ hash, data: blockToStore });
    hashes.push(hash);
    blockCompression.push(actualCompression);

    if (blocksToWrite.length >= 8) {
      await limit(() => writeBlocksBatch(blocksDir, blocksToWrite.splice(0), { verbose }));
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