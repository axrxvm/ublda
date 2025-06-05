import crypto from 'crypto';

// Lightweight array-based hash cache (max 2000 entries)
const hashCache = [];
const MAX_HASHES = 2000;

/**
 * Returns SHA-256 hash of a buffer.
 * @param {Buffer} buffer
 * @returns {string}
 */
export function hashBuffer(buffer) {
  if (!(buffer instanceof Buffer)) throw new Error('Input must be a Buffer');
  const sample = buffer.slice(0, Math.min(16, buffer.length)).toString('hex');
  const existing = hashCache.find(entry => entry.sample === sample);
  if (existing) {
    const hash = crypto.createHash('sha256').update(buffer).digest('hex');
    if (hash === existing.hash) return hash;
  }

  const hash = crypto.createHash('sha256').update(buffer).digest('hex');
  hashCache.push({ sample, hash });
  if (hashCache.length > MAX_HASHES) hashCache.shift();
  return hash;
}