import { readFile, writeFile, verifyFile, storeFile, restoreFile } from './src/index.js';
import fs from 'fs-extra';
import path from 'path';

/**
 * Node.js app comparing FS and UBLDA for file processing.
 * Reads input.txt, converts to uppercase, writes output, measures time and resources.
 */
async function main() {
  const inputFile = 'input.txt';
  const outputFsFile = 'output_fs.txt';
  const outputUbldaFile = 'output_ublda.txt';
  const storageDir = './data';
  const blockSize = 8192; // 8KB blocks
  const compress = true; // Enable compression

  try {
    // Validate input file
    const inputExists = await new Promise(resolve => fs.access(inputFile, err => resolve(!err)));
    if (!inputExists) {
      throw new Error(`Input file not found: ${inputFile}`);
    }

    // Ensure storage directories exist
    await fs.ensureDir(`${storageDir}/blocks`);
    await fs.ensureDir(`${storageDir}/manifests`);

    // Results object to store metrics
    const results = { fs: {}, ublda: {} };

    // --- FS Processing ---
    console.log('\nProcessing with FS...');
    let startTime = process.hrtime.bigint();
    let startMemory = process.memoryUsage().rss;

    // Read with fs.readFile
    const inputFsBuffer = await fs.readFile(inputFile);
    // Process (convert to uppercase)
    const modifiedFsContent = inputFsBuffer.toString().toUpperCase();
    const outputFsBuffer = Buffer.from(modifiedFsContent);
    // Write with fs.writeFile
    await fs.writeFile(outputFsFile, outputFsBuffer);

    let endTime = process.hrtime.bigint();
    let endMemory = process.memoryUsage().rss;

    results.fs.time = Number(endTime - startTime) / 1e6; // ns to ms
    results.fs.memory = (endMemory - startMemory) / 1024 / 1024; // Bytes to MB
    results.fs.outputSize = (await fs.stat(outputFsFile)).size; // Bytes

    console.log(`FS completed: ${outputFsFile} written.`);

    // --- UBLDA Processing ---
    console.log('\nProcessing with UBLDA...');
    startTime = process.hrtime.bigint();
    startMemory = process.memoryUsage().rss;

    // Read with UBLDA (replaces fs.readFile)
    const inputUbldaBuffer = await readFile(`${storageDir}/manifests/${inputFile}.manifest.json`, {
      storageDir,
      verbose: true,
    }).catch(async () => {
      console.log(`No manifest found, storing ${inputFile}...`);
      const { manifestPath } = await storeFile(inputFile, {
        storageDir,
        blockSize,
        compress,
        verbose: false,
      });
      return readFile(manifestPath, { storageDir, verbose: true });
    });

    // Process (convert to uppercase)
    const modifiedUbldaContent = inputUbldaBuffer.toString().toUpperCase();
    const outputUbldaBuffer = Buffer.from(modifiedUbldaContent);

    // Write with UBLDA (replaces fs.writeFile)
    const { manifestPath } = await writeFile(outputUbldaFile, outputUbldaBuffer, {
      storageDir,
      blockSize,
      compress,
      verbose: false,
    });

    // Restore to create physical file
    await restoreFile(manifestPath, outputUbldaFile, {
      storageDir,
      verbose: false,
    });

    // Verify output
    await verifyFile(outputUbldaFile, manifestPath, {
      storageDir,
      verbose: false,
    });

    endTime = process.hrtime.bigint();
    endMemory = process.memoryUsage().rss;

    // Calculate UBLDA storage size (blocks + manifest)
    const blockDir = `${storageDir}/blocks`;
    const manifestFile = manifestPath;
    let ubldaStorageSize = (await fs.stat(manifestFile)).size;
    const manifestData = await fs.readJson(manifestFile);
    for (const hash of manifestData.hashes) {
      const blockPath = path.join(blockDir, hash);
      ubldaStorageSize += (await fs.stat(blockPath)).size;
    }

    results.ublda.time = Number(endTime - startTime) / 1e6; // ns to ms
    results.ublda.memory = (endMemory - startMemory) / 1024 / 1024; // Bytes to MB
    results.ublda.outputSize = ubldaStorageSize; // Bytes

    console.log(`UBLDA completed: ${outputUbldaFile} written, restored, and verified.`);

    // --- Display Results ---
    console.log('\n--- Performance Comparison ---');
    console.log(`FS:`);
    console.log(`  Time: ${results.fs.time.toFixed(2)} ms`);
    console.log(`  Memory Usage: ${results.fs.memory.toFixed(2)} MB`);
    console.log(`  Storage Size: ${results.fs.outputSize} bytes`);
    console.log(`UBLDA:`);
    console.log(`  Time: ${results.ublda.time.toFixed(2)} ms`);
    console.log(`  Memory Usage: ${results.ublda.memory.toFixed(2)} MB`);
    console.log(`  Storage Size: ${results.ublda.outputSize} bytes (blocks + manifest)`);

    // --- Why UBLDA? ---
    console.log('\n--- Why Use UBLDA? ---');
    if (results.ublda.outputSize < results.fs.outputSize) {
      console.log(`- Storage Efficiency: UBLDA uses ${((results.fs.outputSize - results.ublda.outputSize) / results.fs.outputSize * 100).toFixed(2)}% less storage due to compression and deduplication.`);
    }
    console.log(`- Data Integrity: UBLDA verifies output matches stored blocks, unlike FS.`);
    console.log(`- Scalability: UBLDA's block-based approach handles large files efficiently.`);
    if (results.ublda.time > results.fs.time) {
      console.log(`- Note: UBLDA may be slower for small files due to block processing and compression, but excels with larger files or repetitive data.`);
    }

  } catch (error) {
    console.error(`Error: ${error.message}`);
    process.exit(1);
  }
}

// Run the app
main();