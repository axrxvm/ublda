import crypto from 'crypto';

// Lightweight hash set for deduplication (max 10,000 entries)
const hashSet = {};
const hashQueue = [];
const MAX_HASHES = 10000;

/**
 * Returns SHA-256 hash of a buffer.
 * @param {Buffer} buffer
 * @returns {string}
 */
export function hashBuffer(buffer) {
  if (!(buffer instanceof Buffer)) throw new Error('Input must be a Buffer');
  const dataStr = buffer.toString('hex');
  if (hashSet[dataStr]) {
    return hashSet[dataStr];
  }
  const hash = crypto.createHash('sha256').update(buffer).digest('hex');
  hashSet[dataStr] = hash;
  hashQueue.push(dataStr);
  if (hashQueue.length > MAX_HASHES) {
    const oldKey = hashQueue.shift();
    delete hashSet[oldKey];
  }
  return hash;
}