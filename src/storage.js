import path from 'path';
import fs from 'fs-extra';

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
  const exists = await new Promise(resolve => fs.access(filePath, err => resolve(!err)));
  if (!exists) {
    await fs.writeFile(filePath, data);
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
  const filePath = path.join(blockDir, hash);
  const exists = await new Promise(resolve => fs.access(filePath, err => resolve(!err)));
  if (!exists) throw new Error(`Block not found: ${filePath}`);
  return fs.readFile(filePath);
}