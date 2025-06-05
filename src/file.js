import fs from 'fs/promises';
import { access, open, constants } from 'fs/promises';
import { Readable } from 'stream';
import zlib from 'zlib';
import { promisify } from 'util';
import { readBlock } from './storage.js';
import { hashBuffer } from './hasher.js';

const inflateAsync = promisify(zlib.inflate);
const brotliDecompressAsync = promisify(zlib.brotliDecompress);

const COMPRESSION_ALGORITHMS = {
  DEFLATE: 'deflate',
  BROTLI: 'brotli',
  NONE: 'none',
};

/**
 * Splits a file into blocks.
 * @param {string} filePath
 * @param {number} blockSize
 * @returns {Promise<Buffer[]>}
 */
export async function splitFile(filePath, blockSize) {
  try {
    await access(filePath, constants.F_OK);
  } catch {
    throw new Error(`File not found: ${filePath}`);
  }

  if (blockSize <= 0) throw new Error('Block size must be positive');

  const fh = await open(filePath, 'r');
  const { size } = await fh.stat();
  const blocks = [];
  let offset = 0;

  while (offset < size) {
    const len = Math.min(blockSize, size - offset);
    const buffer = Buffer.alloc(len);
    await fh.read(buffer, 0, len, offset);
    blocks.push(buffer);
    offset += len;
  }

  await fh.close();
  return blocks;
}

/**
 * Combines block buffers and writes to output file.
 * @param {string} outputPath
 * @param {Buffer[]} blocks
 */
export async function writeFileFromBlocks(outputPath, blocks) {
  if (!outputPath) throw new Error('Output path is required');
  if (!Array.isArray(blocks) || blocks.some(b => !(b instanceof Buffer))) throw new Error('Blocks must be an array of Buffers');

  const fh = await fs.open(outputPath, 'w');
  for (const block of blocks) {
    await fh.write(block);
  }
  await fh.close();
}

/**
 * Creates a readable stream from blocks.
 * @param {string} blockDir
 * @param {string[]} hashes
 * @param {object} [options]
 * @returns {Readable}
 */
export function createReadStreamFromBlocks(blockDir, hashes, {
  compress = false,
  compressionAlgorithm = COMPRESSION_ALGORITHMS.DEFLATE,
  verbose = false,
  blockCompression = [],
} = {}) {
  let index = 0;

  return new Readable({
    async read() {
      if (index >= hashes.length) {
        this.push(null);
        return;
      }

      try {
        const hash = hashes[index];
        const compression = blockCompression[index] || (compress ? compressionAlgorithm : COMPRESSION_ALGORITHMS.NONE);
        const rawBlock = await readBlock(blockDir, hash);

        let dataToPush = rawBlock;
        if (compression !== COMPRESSION_ALGORITHMS.NONE) {
          dataToPush = compression === COMPRESSION_ALGORITHMS.BROTLI
            ? await brotliDecompressAsync(rawBlock)
            : await inflateAsync(rawBlock);
        }

        this.push(dataToPush);
        index++;
      } catch (error) {
        this.emit('error', new Error(`Failed to stream block ${index + 1}: ${error.message}`));
      }
    }
  });
}