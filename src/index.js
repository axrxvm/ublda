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
const limit = pLimit(5); // Reduced from 10 to lower memory usage

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
} = {}) {
  if (!(await pathExists(filePath))) throw new Error(`File not found: ${filePath}`);
  if (blockSize <= 0) throw new Error('Block size must be positive');

  const fileName = path.basename(filePath);
  const blockDir = path.join(storageDir, 'blocks');
  const manifestDir = path.join(storageDir, 'manifests');
  await fs.ensureDir(blockDir);
  await fs.ensureDir(manifestDir);

  const blocks = await splitFile(filePath, blockSize);
  const hashes = [];
  const metadata = { 
    originalSize: (await fs.stat(filePath)).size, 
    blockSize, // Store block size
    compress, 
    originalName: fileName,
    createdAt: new Date().toISOString()
  };

  for (let i = 0; i < blocks.length; i++) {
    let block = blocks[i];
    if (compress) block = await deflateAsync(block);
    const hash = hashBuffer(block);
    await limit(() => writeBlock(blockDir, hash, block, { verbose }));
    hashes.push(hash);
    if (verbose) console.log(`Stored block ${i + 1}/${blocks.length} (hash: ${hash})`);
  }

  const manifestPath = await saveManifest(manifestDir, fileName, { hashes, metadata });
  return { manifestPath, blockCount: hashes.length };
}

/**
 * Restore file from manifest and blocks.
 * @param {string} manifestPath
 * @param {string} outputPath
 * @param {object} [options]
 */
export async function restoreFile(manifestPath, outputPath, {
  storageDir = './storage',
  verifyHashes = false,
  verbose = false,
} = {}) {
  if (!(await pathExists(manifestPath))) throw new Error(`Manifest file not found: ${manifestPath}`);
  const blockDir = path.join(storageDir, 'blocks');
  if (!(await pathExists(blockDir))) throw new Error(`Block directory not found: ${blockDir}`);

  const { hashes, metadata } = await loadManifest(manifestPath);
  if (!Array.isArray(hashes) || hashes.length === 0) throw new Error('Manifest is empty or invalid');

  const blocks = [];
  for (let i = 0; i < hashes.length; i++) {
    const hash = hashes[i];
    const block = await limit(() => readBlock(blockDir, hash));
    let decompressed = metadata.compress ? await inflateAsync(block) : block;
    if (verifyHashes) {
      const computedHash = hashBuffer(block);
      if (computedHash !== hash) throw new Error(`Hash mismatch for block ${i + 1}: expected ${hash}, got ${computedHash}`);
    }
    blocks.push(decompressed);
    if (verbose) console.log(`Read block ${i + 1}/${hashes.length} (hash: ${hash})`);
  }

  await writeFileFromBlocks(outputPath, blocks);
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
    return createReadStreamFromBlocks(blockDir, hashes, { compress: metadata.compress, verifyHashes, verbose });
  }

  const blocks = [];
  for (let i = 0; i < hashes.length; i++) {
    const hash = hashes[i];
    const block = await limit(() => readBlock(blockDir, hash));
    let decompressed = metadata.compress ? await inflateAsync(block) : block;
    if (verifyHashes) {
      const computedHash = hashBuffer(block);
      if (computedHash !== hash) throw new Error(`Hash mismatch for block ${i + 1}: expected ${hash}, got ${computedHash}`);
    }
    blocks.push(decompressed);
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
  const metadata = { originalSize: buffer.length, blockSize, compress, originalName: filename, createdAt: new Date().toISOString() };

  for (let offset = 0; offset < buffer.length; offset += blockSize) {
    const chunk = buffer.slice(offset, offset + blockSize);
    let block = chunk;
    if (compress) block = await deflateAsync(chunk);
    const hash = hashBuffer(block);
    await limit(() => writeBlock(blocksDir, hash, block, { verbose }));
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

  const { hashes, metadata } = await loadManifest(manifestPath);
  if (!metadata.blockSize) throw new Error('Manifest missing blockSize metadata');

  const fileBuffer = await fs.readFile(filePath);
  if (fileBuffer.length !== metadata.originalSize) throw new Error(`File size mismatch: expected ${metadata.originalSize}, got ${fileBuffer.length}`);

  const blocks = await splitFile(filePath, metadata.blockSize);
  if (blocks.length !== hashes.length) throw new Error(`Block count mismatch: expected ${hashes.length}, got ${blocks.length}`);

  const blockDir = path.join(storageDir, 'blocks');
  for (let i = 0; i < hashes.length; i++) {
    const block = metadata.compress ? await deflateAsync(blocks[i]) : blocks[i];
    const computedHash = hashBuffer(block);
    const storedBlock = await readBlock(blockDir, hashes[i]);
    const storedHash = hashBuffer(storedBlock);
    if (computedHash !== storedHash) throw new Error(`Hash mismatch for block ${i + 1}: expected ${hashes[i]}, got ${computedHash}`);
    if (verbose) console.log(`Verified block ${i + 1}/${hashes.length} (hash: ${hashes[i]})`);
  }

  return true;
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