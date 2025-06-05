import { storeFile, restoreFile, readFile, verifyFile, cleanupBlocks } from './index.js';
import fs from 'fs-extra';
import path from 'path';
import crypto from 'crypto';
import { fileURLToPath } from 'url';

// Recreate __dirname for ES modules
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const TEST_STORAGE_DIR = path.join(__dirname, 'test-storage');
const INPUT_DIR = path.join(__dirname, 'test-input-files'); // Changed name to avoid potential conflict
const DUMMY_FILES_DIR = path.join(INPUT_DIR, 'dummy-files');

// Helper function to create a dummy file with specific content or size
async function createDummyFile(filePath, contentOrSize) {
  await fs.ensureDir(path.dirname(filePath));
  if (typeof contentOrSize === 'string') {
    await fs.writeFile(filePath, contentOrSize);
  } else if (typeof contentOrSize === 'number') {
    await fs.writeFile(filePath, Buffer.alloc(contentOrSize, 'a')); // Fill with 'a'
  } else {
    throw new Error('Invalid contentOrSize: must be string or number');
  }
  return filePath;
}

// Helper function to compare file contents
async function compareFileContents(filePath1, filePath2) {
  const buffer1 = await fs.readFile(filePath1);
  const buffer2 = await fs.readFile(filePath2);
  return Buffer.compare(buffer1, buffer2) === 0;
}

// Helper function to get file hash
async function getFileHash(filePath) {
  const buffer = await fs.readFile(filePath);
  return crypto.createHash('sha256').update(buffer).digest('hex');
}


describe('File Storage System (ublda)', () => {
  beforeAll(async () => {
    // Ensure clean state before all tests
    await fs.remove(TEST_STORAGE_DIR);
    await fs.remove(INPUT_DIR);
    await fs.ensureDir(TEST_STORAGE_DIR);
    await fs.ensureDir(DUMMY_FILES_DIR);
  });

  afterEach(async () => {
    // Clean up storage and dummy files after each test to ensure isolation
    await fs.emptyDir(TEST_STORAGE_DIR);
    // For dummy files, it might be better to clean them per test or rely on beforeAll for a full clean.
    // Let's clean dummy files created specifically for a test within the test or in afterEach.
    // For now, this afterEach will mostly focus on TEST_STORAGE_DIR.
    // If tests create files in DUMMY_FILES_DIR, they should clean them up or use unique names.
  });

  afterAll(async () => {
    // Final cleanup
    await fs.remove(TEST_STORAGE_DIR);
    await fs.remove(INPUT_DIR);
  });

  // Test suite for storeFile and restoreFile
  describe('Store and Restore Operations', () => {
    const testCases = [
      { name: 'empty file, no compression', content: '', compress: false, algo: 'none', file: 'empty.txt' },
      { name: 'small text file, no compression', content: 'Hello, UBLDA! This is a test file.', compress: false, algo: 'none', file: 'small-nocompress.txt' },
      { name: 'small text file, deflate', content: 'Hello, UBLDA with Deflate!', compress: true, algo: 'deflate', file: 'small-deflate.txt' },
      { name: 'small text file, brotli', content: 'Hello, UBLDA with Brotli!', compress: true, algo: 'brotli', file: 'small-brotli.txt' },
      {
        name: 'multi-block file (approx 10KB), deflate',
        content: crypto.randomBytes(10 * 1024).toString('hex'), // ~20KB string from 10KB random bytes
        compress: true,
        algo: 'deflate',
        file: 'multiblock-deflate.bin',
        blockSize: 4096
      },
      {
        name: 'multi-block file (approx 10KB), brotli',
        content: crypto.randomBytes(10 * 1024).toString('hex'),
        compress: true,
        algo: 'brotli',
        file: 'multiblock-brotli.bin',
        blockSize: 4096
      },
       {
        name: 'exactly one block file, deflate',
        content: crypto.randomBytes(4096).toString('hex'),
        compress: true,
        algo: 'deflate',
        file: 'oneblock-deflate.bin',
        blockSize: 4096
      },
    ];

    testCases.forEach(({ name, content, compress, algo, file, blockSize }) => {
      // Isolate the failing test using .only
      // const testFn = (name === 'multi-block file (approx 10KB), brotli') ? test.only : test;
      const testFn = test; // Run all tests
      testFn(`should store and restore an ${name}`, async () => {
        const dummyFilePath = await createDummyFile(path.join(DUMMY_FILES_DIR, file), content);
        const originalFileHash = await getFileHash(dummyFilePath);
        const fileSize = (await fs.stat(dummyFilePath)).size;

        const storeOptions = {
          storageDir: TEST_STORAGE_DIR,
          compress: compress,
          compressionAlgorithm: algo === 'none' ? undefined : algo, // Pass undefined if algo is 'none'
          blockSize: blockSize || 4096, // Use specified blockSize or default
          // verbose: name === 'multi-block file (approx 10KB), brotli', // Enable verbose for the failing test
          verbose: false, // Disable verbose logging for all tests
        };

        const { manifestPath, blockCount } = await storeFile(dummyFilePath, storeOptions);

        expect(await fs.pathExists(manifestPath)).toBe(true);

        const manifestData = await fs.readJson(manifestPath);
        expect(manifestData.metadata.originalSize).toBe(fileSize);
        expect(manifestData.metadata.compress).toBe(compress);
        // storeFile now sets compressionAlgorithm to 'none' if !compress
        expect(manifestData.metadata.compressionAlgorithm).toBe(compress ? algo : 'none');
        expect(manifestData.hashes.length).toBe(blockCount);
        if (fileSize === 0) {
            expect(blockCount).toBe(0);
        } else {
            expect(blockCount).toBeGreaterThan(0);
            // Check if block files exist
            for (const hash of manifestData.hashes) {
                expect(await fs.pathExists(path.join(TEST_STORAGE_DIR, 'blocks', hash))).toBe(true);
            }
        }


        const restoredFilePath = path.join(DUMMY_FILES_DIR, `${file}.restored`);
        await restoreFile(manifestPath, restoredFilePath, {
            storageDir: TEST_STORAGE_DIR,
            // verbose: name === 'multi-block file (approx 10KB), brotli', // Enable verbose for the failing test
            verbose: false, // Disable verbose logging for all tests
            verifyHashes: true // Test with hash verification
        });

        expect(await compareFileContents(dummyFilePath, restoredFilePath)).toBe(true);
        const restoredFileHash = await getFileHash(restoredFilePath);
        expect(restoredFileHash).toBe(originalFileHash);

        await fs.remove(dummyFilePath);
        await fs.remove(restoredFilePath);
      });
    });
  });

  // Old test, can be removed or kept if it tests something unique not covered by table
  describe('Basic Store and Restore (Legacy, can be merged or removed)', () => {
    test('should store and restore a small text file without compression', async () => {
      const content = 'Hello, UBLDA! This is a test file.';
      const dummyFilePath = await createDummyFile(path.join(DUMMY_FILES_DIR, 'small-nocompress-legacy.txt'), content);
      const originalFileHash = await getFileHash(dummyFilePath);

      const options = {
        storageDir: TEST_STORAGE_DIR,
        compress: false,
        verbose: false,
      };

      const { manifestPath, blockCount } = await storeFile(dummyFilePath, options);

      expect(await fs.pathExists(manifestPath)).toBe(true);
      expect(blockCount).toBeGreaterThan(0);

      const manifestData = await fs.readJson(manifestPath);
      expect(manifestData.metadata.originalSize).toBe(Buffer.byteLength(content));
      expect(manifestData.metadata.compress).toBe(false);
      expect(manifestData.metadata.compressionAlgorithm).toBe('none');

      const restoredFilePath = path.join(DUMMY_FILES_DIR, 'small-nocompress-legacy-restored.txt');
      await restoreFile(manifestPath, restoredFilePath, { storageDir: TEST_STORAGE_DIR, verbose: false, verifyHashes: true });

      expect(await compareFileContents(dummyFilePath, restoredFilePath)).toBe(true);
      const restoredFileHash = await getFileHash(restoredFilePath);
      expect(restoredFileHash).toBe(originalFileHash);

      await fs.remove(dummyFilePath);
      await fs.remove(restoredFilePath);
    });
  });

  describe('readFile Tests', () => {
    // Test cases for different compression settings
    const readFileTestCases = [
      { compress: false, algo: 'none', content: 'Readable file content without compression.', fileSuffix: 'read-nocompress.txt' },
      { compress: true, algo: 'deflate', content: 'Readable file content with deflate.', fileSuffix: 'read-deflate.txt' },
      { compress: true, algo: 'brotli', content: 'Readable file content with brotli.', fileSuffix: 'read-brotli.txt' },
    ];

    readFileTestCases.forEach(({ compress, algo, content, fileSuffix }) => {
      const testName = `should read file content (compress: ${compress}, algo: ${algo || 'default'})`;
      const dummyFileName = `source-${fileSuffix}`;
      const dummyFilePath = path.join(DUMMY_FILES_DIR, dummyFileName);

      // Test for readFile (non-stream)
      test(`${testName} - non-stream`, async () => {
        await createDummyFile(dummyFilePath, content);

        const storeOptions = {
          storageDir: TEST_STORAGE_DIR,
          compress: compress,
          compressionAlgorithm: algo === 'none' ? undefined : algo,
        };
        const { manifestPath } = await storeFile(dummyFilePath, storeOptions);

        const readBuffer = await readFile(manifestPath, { storageDir: TEST_STORAGE_DIR, verifyHashes: true });
        expect(readBuffer.toString()).toBe(content);

        await fs.remove(dummyFilePath);
      });

      // Test for readFile (stream)
      test(`${testName} - stream`, async () => {
        await createDummyFile(dummyFilePath, content);

        const storeOptions = {
          storageDir: TEST_STORAGE_DIR,
          compress: compress,
          compressionAlgorithm: algo === 'none' ? undefined : algo,
        };
        const { manifestPath } = await storeFile(dummyFilePath, storeOptions);

        const stream = await readFile(manifestPath, { storageDir: TEST_STORAGE_DIR, stream: true, verifyHashes: true });

        let streamedContent = '';
        for await (const chunk of stream) {
          streamedContent += chunk.toString();
        }
        expect(streamedContent).toBe(content);

        await fs.remove(dummyFilePath);
      });
    });
  });

  describe('verifyFile Tests', () => {
    const verifyTestCases = [
      { compress: false, algo: 'none', content: 'Content for verifyFile, no compression.', file: 'verify-nocompress.txt' },
      { compress: true, algo: 'deflate', content: 'Content for verifyFile, deflate.', file: 'verify-deflate.txt' },
      { compress: true, algo: 'brotli', content: 'Content for verifyFile, brotli.', file: 'verify-brotli.txt' },
      { compress: false, algo: 'none', content: '', file: 'verify-empty.txt' }, // Empty file
    ];

    verifyTestCases.forEach(({ compress, algo, content, file }) => {
      const testName = `verifyFile (compress: ${compress}, algo: ${algo || 'default'}, file: ${file})`;
      const dummyFilePath = path.join(DUMMY_FILES_DIR, file);

      test(`${testName} - valid file`, async () => {
        await createDummyFile(dummyFilePath, content);
        const storeOptions = { storageDir: TEST_STORAGE_DIR, compress, compressionAlgorithm: algo === 'none' ? undefined : algo };
        const { manifestPath } = await storeFile(dummyFilePath, storeOptions);

        const isValid = await verifyFile(dummyFilePath, manifestPath, { storageDir: TEST_STORAGE_DIR });
        expect(isValid).toBe(true);

        await fs.remove(dummyFilePath);
      });

      if (content !== '') { // Negative tests don't make sense for an empty file in the same way
        test(`${testName} - file content modified`, async () => {
          await createDummyFile(dummyFilePath, content);
          const storeOptions = { storageDir: TEST_STORAGE_DIR, compress, compressionAlgorithm: algo === 'none' ? undefined : algo };
          const { manifestPath } = await storeFile(dummyFilePath, storeOptions);

          await fs.writeFile(dummyFilePath, content + ' - modified'); // Modify content

          await expect(verifyFile(dummyFilePath, manifestPath, { storageDir: TEST_STORAGE_DIR }))
            .rejects.toThrow(/Hash mismatch|File size mismatch/); // Error message might vary based on where it detects first

          await fs.remove(dummyFilePath);
        });

        test(`${testName} - manifest hash incorrect`, async () => {
          await createDummyFile(dummyFilePath, content);
          const storeOptions = { storageDir: TEST_STORAGE_DIR, compress, compressionAlgorithm: algo === 'none' ? undefined : algo };
          const { manifestPath } = await storeFile(dummyFilePath, storeOptions);

          const manifestData = await fs.readJson(manifestPath);
          manifestData.hashes[0] = 'incorrecthash' + manifestData.hashes[0].substring(13); // Modify a hash
          await fs.writeJson(manifestPath, manifestData);

          await expect(verifyFile(dummyFilePath, manifestPath, { storageDir: TEST_STORAGE_DIR }))
            .rejects.toThrow(/Hash mismatch|Stored block.*corrupted/);

          await fs.remove(dummyFilePath);
        });

        test(`${testName} - block missing from storage`, async () => {
          await createDummyFile(dummyFilePath, content);
          const storeOptions = { storageDir: TEST_STORAGE_DIR, compress, compressionAlgorithm: algo === 'none' ? undefined : algo };
          const { manifestPath } = await storeFile(dummyFilePath, storeOptions);

          const manifestData = await fs.readJson(manifestPath);
          if (manifestData.hashes.length > 0) {
            await fs.remove(path.join(TEST_STORAGE_DIR, 'blocks', manifestData.hashes[0])); // Remove a block

            await expect(verifyFile(dummyFilePath, manifestPath, { storageDir: TEST_STORAGE_DIR }))
              .rejects.toThrow(/Block not found/); // Updated to match error from readBlock
          }
          await fs.remove(dummyFilePath);
        });
      }
       test(`${testName} - manifest originalSize incorrect`, async () => {
          await createDummyFile(dummyFilePath, content);
          const storeOptions = { storageDir: TEST_STORAGE_DIR, compress, compressionAlgorithm: algo === 'none' ? undefined : algo };
          const { manifestPath } = await storeFile(dummyFilePath, storeOptions);

          const manifestData = await fs.readJson(manifestPath);
          manifestData.metadata.originalSize += 10; // Modify originalSize
          await fs.writeJson(manifestPath, manifestData);

          let expectedError = /File size mismatch/;
          if (content === '') { // If it was an empty file, and size is now > 0 with no hashes
            expectedError = /Manifest contains no hashes for a non-empty file./;
          }
          await expect(verifyFile(dummyFilePath, manifestPath, { storageDir: TEST_STORAGE_DIR }))
            .rejects.toThrow(expectedError);

          await fs.remove(dummyFilePath);
        });
    });
  });

  describe('Backward Compatibility Tests', () => {
    test('should restore a file with old manifest format (no compressionAlgorithm field)', async () => {
      const content = "Old manifest format test with deflate.";
      const dummyFilePath = await createDummyFile(path.join(DUMMY_FILES_DIR, 'old-manifest-src.txt'), content);
      const originalFileHash = await getFileHash(dummyFilePath);

      // Manually simulate storing with an "old" version that used deflate
      const tempStoreOptions = { storageDir: TEST_STORAGE_DIR, compress: true, compressionAlgorithm: 'deflate' };
      const { manifestPath: tempManifestPath } = await storeFile(dummyFilePath, tempStoreOptions);

      // Modify the manifest to remove compressionAlgorithm
      const manifestData = await fs.readJson(tempManifestPath);
      delete manifestData.metadata.compressionAlgorithm;
      // Ensure compress is true
      manifestData.metadata.compress = true;
      const oldFormatManifestPath = path.join(TEST_STORAGE_DIR, 'manifests', 'old-format.manifest.json');
      await fs.writeJson(oldFormatManifestPath, manifestData);
      await fs.remove(tempManifestPath); // remove the temp manifest

      // Test restoreFile
      const restoredFilePath = path.join(DUMMY_FILES_DIR, 'old-manifest-restored.txt');
      await restoreFile(oldFormatManifestPath, restoredFilePath, { storageDir: TEST_STORAGE_DIR, verifyHashes: true });
      expect(await compareFileContents(dummyFilePath, restoredFilePath)).toBe(true);

      // Test readFile (non-stream)
      const buffer = await readFile(oldFormatManifestPath, { storageDir: TEST_STORAGE_DIR, verifyHashes: true });
      expect(buffer.toString()).toBe(content);

      // Test readFile (stream)
      const stream = await readFile(oldFormatManifestPath, { storageDir: TEST_STORAGE_DIR, stream: true, verifyHashes: true });
      let streamedContent = '';
      for await (const chunk of stream) { streamedContent += chunk.toString(); }
      expect(streamedContent).toBe(content);

      await fs.remove(dummyFilePath);
      await fs.remove(restoredFilePath);
    });
  });

  describe('Error Handling and Edge Cases', () => {
    test('storeFile with non-existent input file should throw', async () => {
      await expect(storeFile(path.join(INPUT_DIR, 'nonexistent.txt'), { storageDir: TEST_STORAGE_DIR }))
        .rejects.toThrow(/File not found/);
    });

    test('restoreFile with non-existent manifest should throw', async () => {
      await expect(restoreFile(path.join(TEST_STORAGE_DIR, 'manifests', 'nonexistent.manifest.json'), 'output.txt', { storageDir: TEST_STORAGE_DIR }))
        .rejects.toThrow(/Manifest file not found/);
    });

    test('readFile with non-existent manifest should throw', async () => {
      await expect(readFile(path.join(TEST_STORAGE_DIR, 'manifests', 'nonexistent.manifest.json'), { storageDir: TEST_STORAGE_DIR }))
        .rejects.toThrow(/Manifest file not found/);
    });

    test('storeFile with blockSize 0 should throw', async () => {
      const dummyFilePath = await createDummyFile(path.join(DUMMY_FILES_DIR, 'blocksize-zero.txt'), "test");
      await expect(storeFile(dummyFilePath, { storageDir: TEST_STORAGE_DIR, blockSize: 0 }))
        .rejects.toThrow(/Block size must be positive/);
      await fs.remove(dummyFilePath);
    });

    test('storeFile with negative blockSize should throw', async () => {
      const dummyFilePath = await createDummyFile(path.join(DUMMY_FILES_DIR, 'blocksize-neg.txt'), "test");
      await expect(storeFile(dummyFilePath, { storageDir: TEST_STORAGE_DIR, blockSize: -100 }))
        .rejects.toThrow(/Block size must be positive/);
      await fs.remove(dummyFilePath);
    });

    test('verifyFile with non-existent local file should throw', async () => {
        const dummyFilePath = await createDummyFile(path.join(DUMMY_FILES_DIR, 'verify-nonlocal-setup.txt'), "content");
        const { manifestPath } = await storeFile(dummyFilePath, { storageDir: TEST_STORAGE_DIR });
        await fs.remove(dummyFilePath); // remove local file after storing

        await expect(verifyFile(dummyFilePath, manifestPath, { storageDir: TEST_STORAGE_DIR }))
            .rejects.toThrow(/File not found/);
    });

    test('verifyFile with non-existent manifest file should throw', async () => {
        const dummyFilePath = await createDummyFile(path.join(DUMMY_FILES_DIR, 'verify-nonmanifest-setup.txt'), "content");
        // Don't actually store, just need a file path
        await expect(verifyFile(dummyFilePath, path.join(TEST_STORAGE_DIR, 'manifests', 'nonexistent.manifest.json'), { storageDir: TEST_STORAGE_DIR }))
            .rejects.toThrow(/Manifest file not found/);
        await fs.remove(dummyFilePath);
    });

  });

  describe('cleanupBlocks tests', () => {
    test('should cleanup unused blocks', async () => {
      const file1Content = "File 1 content for cleanup test.";
      const file1Path = await createDummyFile(path.join(DUMMY_FILES_DIR, 'cleanup1.txt'), file1Content);
      const file2Content = "File 2 content, shares some blocks maybe, maybe not.";
      const file2Path = await createDummyFile(path.join(DUMMY_FILES_DIR, 'cleanup2.txt'), file2Content);

      // Store file1
      const { manifestPath: manifest1Path, blockCount: blockCount1 } = await storeFile(file1Path, {
        storageDir: TEST_STORAGE_DIR, compress: true, compressionAlgorithm: 'deflate'
      });
      expect(blockCount1).toBeGreaterThan(0);
      const manifest1Data = await fs.readJson(manifest1Path);
      const initialBlockFiles = await fs.readdir(path.join(TEST_STORAGE_DIR, 'blocks'));
      expect(initialBlockFiles.length).toBe(manifest1Data.hashes.length);


      // Store file2 (might share blocks or have new ones)
      const { manifestPath: manifest2Path, blockCount: blockCount2 } = await storeFile(file2Path, {
        storageDir: TEST_STORAGE_DIR, compress: true, compressionAlgorithm: 'deflate'
      });
      const manifest2Data = await fs.readJson(manifest2Path);
      const totalHashes = new Set([...manifest1Data.hashes, ...manifest2Data.hashes]);
      const blocksAfterFile2 = await fs.readdir(path.join(TEST_STORAGE_DIR, 'blocks'));
      expect(blocksAfterFile2.length).toBe(totalHashes.size);

      // Delete manifest for file1 (simulating file deletion)
      await fs.remove(manifest1Path);

      const deletedCount = await cleanupBlocks(TEST_STORAGE_DIR, { verbose: false });

      const blocksAfterCleanup = await fs.readdir(path.join(TEST_STORAGE_DIR, 'blocks'));
      // Expected remaining blocks are those only in manifest2 that were not in manifest1
      // or shared blocks that are still referenced by manifest2
      const expectedRemainingHashes = new Set(manifest2Data.hashes);
      expect(blocksAfterCleanup.length).toBe(expectedRemainingHashes.size);

      // The number of deleted blocks should be the difference in unique hashes
      // between (hashes_in_manifest1 U hashes_in_manifest2) and (hashes_in_manifest2)
      // which simplifies to | unique_hashes_in_manifest1 - unique_hashes_in_manifest2_that_were_also_in_1 |
      const uniqueHashesInManifest1 = new Set(manifest1Data.hashes);
      let expectedDeletedCount = 0;
      uniqueHashesInManifest1.forEach(hash => {
        if (!expectedRemainingHashes.has(hash)) {
          expectedDeletedCount++;
        }
      });
      expect(deletedCount).toBe(expectedDeletedCount);

      await fs.remove(file1Path);
      await fs.remove(file2Path);
    });
  });
});
