import crypto from 'node:crypto';

/**
 * Returns SHA-256 hash of a buffer.
 * @param {Buffer} buffer
 * @returns {string}
 */
export function hashBuffer(buffer) {
  if (!(buffer instanceof Buffer)) throw new Error('Input must be a Buffer');
  return crypto.createHash('sha256').update(buffer).digest('hex');
}