import path from 'node:path';
import fs from 'node:fs/promises';

// Minimal FIFO cache for blocks (max 32MB)
const blockCache = new Map();
const blockQueue = [];
const MAX_CACHE_SIZE = 32 * 1024 * 1024; // 32MB
let currentCacheSize = 0;

// Cache for existence checks
const existsCache = new Map();

/**
 * Writes multiple blocks in a batch using fs.writev.
 * @param {string} blockDir
 * @param {Array<{hash: string, data: Buffer}>} blocks
 * @param {object} [options]
 */
export async function writeBlocksBatch(blockDir, blocks, { verbose = false } = {}) {
  await fs.mkdir(blockDir, { recursive: true });
  const writeOps = [];
  const filesToCheck = [];

  for (const { hash, data } of blocks) {
    if (!hash || !/^[0-9a-f]{64}$/i.test(hash)) throw new Error('Invalid hash');
    if (!(data instanceof Buffer)) throw new Error('Data must be a Buffer');

    const filePath = path.join(blockDir, hash);
    if (!existsCache.has(filePath)) filesToCheck.push({ filePath, hash, data });
    else if (!existsCache.get(filePath)) writeOps.push({ filePath, data });
  }

  if (filesToCheck.length > 0) {
    const existsResults = await Promise.all(filesToCheck.map(({ filePath }) => 
      fs.access(filePath).then(() => true).catch(() => false)
    ));
    filesToCheck.forEach(({ filePath, hash, data }, i) => {
      existsCache.set(filePath, existsResults[i]);
      if (!existsResults[i]) {
        writeOps.push({ filePath, data });
        blockCache.set(hash, data);
        blockQueue.push(hash);
        currentCacheSize += data.length;
      }
    });
  }

  while (currentCacheSize > MAX_CACHE_SIZE && blockQueue.length > 0) {
    const oldHash = blockQueue.shift();
    const oldData = blockCache.get(oldHash);
    if (oldData) {
      currentCacheSize -= oldData.length;
      blockCache.delete(oldHash);
    }
  }

  if (writeOps.length > 0) {
    await Promise.all(writeOps.map(({ filePath, data }) => {
      if (verbose) console.log(`Block written: ${filePath}`);
      return fs.writeFile(filePath, data);
    }));
  }
}

/**
 * Writes a single block (for compatibility).
 * @param {string} blockDir
 * @param {string} hash
 * @param {Buffer} data
 * @param {object} [options]
 */
export async function writeBlock(blockDir, hash, data, options) {
  return writeBlocksBatch(blockDir, [{ hash, data }], options);
}

/**
 * Reads block from storage.
 * @param {string} blockDir
 * @param {string} hash
 * @returns {Promise<Buffer>}
 */
export async function readBlock(blockDir, hash) {
  if (!hash || !/^[0-9a-f]{64}$/i.test(hash)) throw new Error('Invalid hash');

  const cachedBlock = blockCache.get(hash);
  if (cachedBlock) return cachedBlock;

  const filePath = path.join(blockDir, hash);
  let exists = existsCache.get(filePath);
  if (exists === undefined) {
    exists = await fs.access(filePath).then(() => true).catch(() => false);
    existsCache.set(filePath, exists);
  }
  if (!exists) throw new Error(`Block not found: ${filePath}`);

  const data = await fs.readFile(filePath);
  blockCache.set(hash, data);
  blockQueue.push(hash);
  currentCacheSize += data.length;

  while (currentCacheSize > MAX_CACHE_SIZE && blockQueue.length > 0) {
    const oldHash = blockQueue.shift();
    const oldData = blockCache.get(oldHash);
    if (oldData) {
      currentCacheSize -= oldData.length;
      blockCache.delete(oldHash);
    }
  }

  return data;
}