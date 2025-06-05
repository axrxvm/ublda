import path from 'path';
import fs from 'fs-extra';
import { LRUCache } from 'lru-cache';

// Initialize LRU cache for frequently accessed blocks (10MB max)
const blockCache = new LRUCache({
  maxSize: 10 * 1024 * 1024, // 10MB
  sizeCalculation: (value) => value.length,
});

/**
 * Writes block if it doesn't exist.
 * @param {string} blockDir
 * @param {string} hash
 * @param {Buffer} data
 * @param {object} [options]
 */
export async function writeBlock(blockDir, hash, data, { verbose = false } = {}) {
  if (!hash || !/^[0-9a-f]{64}$/i.test(hash)) throw new Error('Invalid hash');
  if (!(data instanceof Buffer)) throw new Error('Data must be a Buffer');

  await fs.ensureDir(blockDir);
  const filePath = path.join(blockDir, hash);
  const exists = await fs.access(filePath).then(() => true).catch(() => false);
  if (!exists) {
    await fs.writeFile(filePath, data);
    blockCache.set(hash, data); // Cache the block
    if (verbose) console.log(`Block written: ${filePath}`);
  }
}

/**
 * Reads block from storage.
 * @param {string} blockDir
 * @param {string} hash
 * @returns {Promise<Buffer>}
 */
export async function readBlock(blockDir, hash) {
  if (!hash || !/^[0-9a-f]{64}$/i.test(hash)) throw new Error('Invalid hash');
  
  // Check cache first
  const cachedBlock = blockCache.get(hash);
  if (cachedBlock) return cachedBlock;

  const filePath = path.join(blockDir, hash);
  const exists = await fs.access(filePath).then(() => true).catch(() => false);
  if (!exists) throw new Error(`Block not found: ${filePath}`);
  const data = await fs.readFile(filePath);
  blockCache.set(hash, data); // Cache the block
  return data;
}