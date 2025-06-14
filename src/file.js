import fs from 'node:fs/promises';
import { access, constants } from 'node:fs/promises';
import { Readable } from 'node:stream';
import zlib from 'node:zlib';
import { promisify } from 'node:util';
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
  await access(filePath, constants.F_OK).catch(() => { throw new Error(`File not found: ${filePath}`); });
  if (blockSize <= 0) throw new Error('Block size must be positive');

  return new Promise((resolve, reject) => {
    const blocks = [];
    const stream = fs.createReadStream(filePath, { highWaterMark: blockSize });

    stream.on('data', (chunk) => blocks.push(chunk));
    stream.on('end', () => resolve(blocks));
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
    blocks.forEach(block => outputStream.write(block));
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
  compress = false, // Original option, now less directly used by the stream itself for default
  compressionAlgorithm = COMPRESSION_ALGORITHMS.DEFLATE, // Default if not in blockCompression
  verbose = false,
  blockCompression = [], // Array of compression types per block
  limit = null, // pLimit instance
} = {}) {
  if (!limit) {
    // Fallback to a dummy limiter if none is provided, though actual concurrency won't be achieved.
    // Or, throw an error: throw new Error('pLimit instance (options.limit) is required.');
    // For now, let's make it required.
    throw new Error('pLimit instance (options.limit) is required for createReadStreamFromBlocks.');
  }

  // Helper async generator
  async function* processBlocksGenerator() {
    const taskPromises = [];

    for (let i = 0; i < hashes.length; i++) {
      const hash = hashes[i];
      // Determine the actual compression for this block:
      // 1. Use specific blockCompression[i] if available.
      // 2. Fallback to options.compress (implicitly, via compressionAlgorithm) if not.
      // The `compress` flag itself is less important now than the `blockCompression` array or `compressionAlgorithm`
      const blockSpecificCompression = blockCompression[i] || compressionAlgorithm;

      if (verbose) console.log(`[BlockReadStream] Scheduling block ${i} (hash: ${hash}, compression: ${blockSpecificCompression})`);

      const taskPromise = limit(async () => {
        try {
          if (verbose) console.log(`[BlockReadStream] Reading block ${i} (hash: ${hash})`);
          const rawBlock = await readBlock(blockDir, hash);

          if (blockSpecificCompression === COMPRESSION_ALGORITHMS.NONE) {
            if (verbose) console.log(`[BlockReadStream] Finished block ${i} (hash: ${hash}, no decompression)`);
            return rawBlock;
          }

          if (verbose) console.log(`[BlockReadStream] Decompressing block ${i} (hash: ${hash}, method: ${blockSpecificCompression})`);
          const decompressedData = blockSpecificCompression === COMPRESSION_ALGORITHMS.BROTLI
            ? await brotliDecompressAsync(rawBlock)
            : await inflateAsync(rawBlock);
          if (verbose) console.log(`[BlockReadStream] Finished block ${i} (hash: ${hash})`);
          return decompressedData;
        } catch (error) {
          // Decorate error with more context before it's re-thrown by limit and caught by generator's consumer
          const contextError = new Error(`Error processing block ${i} (hash: ${hash}): ${error.message}`);
          contextError.stack = error.stack; // Preserve original stack
          contextError.originalError = error;
          throw contextError;
        }
      });
      taskPromises.push(taskPromise); // Store promise, not {promise, index} as order is preserved by awaiting this array.
    }

    // Yield promises in order they were added (and should be processed)
    for (let i = 0; i < taskPromises.length; i++) {
      try {
        yield await taskPromises[i];
      } catch (error) {
        // This will catch errors from the limit-wrapped function
        // and propagate them to the stream's error handler via blockProcessor.next()
        throw error; // Re-throw to be caught by the stream's read method
      }
    }
  }

  const blockProcessor = processBlocksGenerator();

  return new Readable({
    async read() {
      try {
        const { value, done } = await blockProcessor.next();
        if (done) {
          this.push(null); // Signal end of stream
        } else {
          if (!this.push(value)) {
            // If push returns false, it means the consumer is not ready (backpressure)
            // The stream will automatically stop calling _read until the consumer is ready again.
            // The generator will pause at the await blockProcessor.next() until read() is called again.
            if (verbose) console.log('[BlockReadStream] Backpressure applied.');
          }
        }
      } catch (e) {
        // Emit error on the stream
        if (verbose) console.error(`[BlockReadStream] Error: ${e.message}`);
        this.emit('error', e);
      }
    }
  });
}