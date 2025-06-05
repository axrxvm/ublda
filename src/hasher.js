import crypto from 'crypto';
import { BloomFilter } from 'bloomfilter';

// Initialize a Bloom filter for deduplication (1M items, 0.01% false positive rate)
const bloom = new BloomFilter(1000000, 10);

/**
 * Returns SHA-256 hash of a buffer.
 * @param {Buffer} buffer
 * @returns {string}
 */
export function hashBuffer(buffer) {
  if (!(buffer instanceof Buffer)) throw new Error('Input must be a Buffer');
  const dataStr = buffer.toString('hex');
  if (bloom.test(dataStr)) {
    // Check if hash likely exists to avoid redundant computation
    const hash = crypto.createHash('sha256').update(buffer).digest('hex');
    return hash;
  }
  const hash = crypto.createHash('sha256').update(buffer).digest('hex');
  bloom.add(dataStr);
  return hash;
}