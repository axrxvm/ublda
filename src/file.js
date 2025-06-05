import fs from 'fs/promises';
import { access, constants } from 'fs/promises';
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
 * Splits a file into blocks using streams.
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

  return new Promise((resolve, reject) => {
    const blocks = [];
    let currentBlock = Buffer.alloc(0);
    const stream = fs.createReadStream(filePath, { highWaterMark: blockSize });

    stream.on('data', (chunk) => {
      if (currentBlock.length + chunk.length <= blockSize) {
        currentBlock = Buffer.concat([currentBlock, chunk]);
      } else {
        blocks.push(currentBlock);
        currentBlock = chunk;
      }
    });

    stream.on('end', () => {
      if (currentBlock.length > 0) blocks.push(currentBlock);
      resolve(blocks);
    });

    stream.on('error', (err) => reject(err));
  });
}

/**
 * Combines block buffers and writes to output file.
 * @param {string} outputPath
 * @param {Buffer[]} blocks
 */
export async function writeFileFromBlocks(outputPath, blocks) {
  if (!outputPath) throw new Error('Output path is required');
  if (!Array.isArray(blocks) || blocks.some(b => !(b instanceof Buffer))) throw new Error('Blocks must be an array of Buffers');

  const outputStream = fs.createWriteStream(outputPath);
  return new Promise((resolve, reject) => {
    outputStream.on('finish', resolve);
    outputStream.on('error', reject);

    for (const block of blocks) {
      if (!outputStream.write(block)) {
        outputStream.once('drain', () => {});
      }
    }
    outputStream.end();
  });
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