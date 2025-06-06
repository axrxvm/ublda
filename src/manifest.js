import path from 'node:path';
import fs from 'fs-extra';

/**
 * Saves a manifest JSON file.
 * @param {string} manifestDir
 * @param {string} filename
 * @param {object} data
 * @returns {Promise<string>}
 */
export async function saveManifest(manifestDir, filename, { hashes, metadata }) {
  if (!filename) throw new Error('Filename is required');
  if (!Array.isArray(hashes)) throw new Error('Hashes must be an array');
  if (!metadata || typeof metadata !== 'object') throw new Error('Metadata must be an object');

  const outPath = path.join(manifestDir, `${filename}.manifest.json`);
  await fs.writeJson(outPath, { hashes, metadata });
  return outPath;
}

/**
 * Loads manifest file.
 * @param {string} manifestPath
 * @returns {Promise<{ hashes: string[], metadata: object }>}
 */
export async function loadManifest(manifestPath) {
  if (!manifestPath.endsWith('.json')) throw new Error('Manifest must be a .json file');
  if (!(await fs.access(manifestPath).then(() => true).catch(() => false))) {
    throw new Error(`Manifest not found: ${manifestPath}`);
  }
  const data = await fs.readJson(manifestPath);
  if (!data.hashes || !data.metadata) throw new Error('Invalid manifest format');
  return data;
}