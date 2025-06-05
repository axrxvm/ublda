import { readFile, writeFile, storeFile, restoreFile } from './src/index.js';
import fs from 'fs-extra';
import path from 'path';

async function main() {
  const inputFile = 'input.txt';
  const outputFsFile = 'output_fs.txt';
  const outputUbldaFile = 'output_ublda.txt';
  const storageDir = './data';
  const blockSize = 4 * 1024 * 1024;
  const compress = true;
  const graphMode = process.argv.includes('--graph'); // 👈 Flag check

  const inputStats = await fs.stat(inputFile);
  const inputSizeMB = (inputStats.size / 1024 / 1024).toFixed(2);
  console.log(`📄 Input File Size: ${inputStats.size} bytes (${inputSizeMB} MB)`);

  try {
    const inputExists = await fs.pathExists(inputFile);
    if (!inputExists) throw new Error(`Input file not found: ${inputFile}`);

    await fs.ensureDir(`${storageDir}/blocks`);
    await fs.ensureDir(`${storageDir}/manifests`);

    const results = { fs: {}, ublda: {} };

    // --- FS Processing ---
    console.log('\n📁 Processing with FS...');
    let startTime = process.hrtime.bigint();
    let startMemory = process.memoryUsage().rss;

    const inputFsBuffer = await fs.readFile(inputFile);
    const modifiedFsContent = inputFsBuffer.toString().toUpperCase();
    await fs.writeFile(outputFsFile, Buffer.from(modifiedFsContent));

    let endTime = process.hrtime.bigint();
    let endMemory = process.memoryUsage().rss;

    results.fs.time = Number(endTime - startTime) / 1e6;
    results.fs.memory = (endMemory - startMemory) / 1024 / 1024;
    results.fs.outputSize = (await fs.stat(outputFsFile)).size;

    console.log(`✅ FS completed: ${outputFsFile} written.`);

    // --- UBLDA Processing ---
    console.log('\n📦 Processing with UBLDA...');
    startTime = process.hrtime.bigint();
    startMemory = process.memoryUsage().rss;

    const inputUbldaBuffer = await readFile(`${storageDir}/manifests/${inputFile}.manifest.json`, {
      storageDir, verbose: false
    }).catch(async () => {
      console.log(`ℹ️  No manifest found. Storing ${inputFile}...`);
      const { manifestPath } = await storeFile(inputFile, {
        storageDir, blockSize, compress, verbose: false
      });
      return readFile(manifestPath, { storageDir, verbose: false });
    });

    const modifiedUbldaContent = inputUbldaBuffer.toString().toUpperCase();
    const outputUbldaBuffer = Buffer.from(modifiedUbldaContent);
    const { manifestPath } = await writeFile(outputUbldaFile, outputUbldaBuffer, {
      storageDir, blockSize, compress, verbose: false
    });
    await restoreFile(manifestPath, outputUbldaFile, {
      storageDir, verbose: false
    });

    endTime = process.hrtime.bigint();
    endMemory = process.memoryUsage().rss;

    let ubldaStorageSize = (await fs.stat(manifestPath)).size;
    const manifestData = await fs.readJson(manifestPath);
    for (const hash of manifestData.hashes) {
      ubldaStorageSize += (await fs.stat(path.join(storageDir, 'blocks', hash))).size;
    }

    results.ublda.time = Number(endTime - startTime) / 1e6;
    results.ublda.memory = (endMemory - startMemory) / 1024 / 1024;
    results.ublda.outputSize = ubldaStorageSize;

    console.log(`✅ UBLDA completed: ${outputUbldaFile} written and restored.`);

    // --- Metric Calculations ---
    const percent = (a, b) => ((a - b) / a) * 100;
    const formatBytes = (bytes) => `${bytes} bytes (${(bytes / 1024 / 1024).toFixed(2)} MB)`;

    const timeDiff = percent(results.fs.time, results.ublda.time);
    const memoryDiff = percent(results.fs.memory, results.ublda.memory);
    const storageDiff = percent(results.fs.outputSize, results.ublda.outputSize);

    if (graphMode) {
      const graphData = {
        input: {
          size_bytes: inputStats.size,
          size_mb: parseFloat(inputSizeMB)
        },
        fs: {
          time_ms: results.fs.time,
          memory_mb: results.fs.memory,
          storage_bytes: results.fs.outputSize
        },
        ublda: {
          time_ms: results.ublda.time,
          memory_mb: results.ublda.memory,
          storage_bytes: results.ublda.outputSize
        },
        comparison: {
          storage_saved_percent: parseFloat(((1 - results.ublda.outputSize / results.fs.outputSize) * 100).toFixed(5)),
          storage_efficiency_ratio: parseFloat((results.fs.outputSize / results.ublda.outputSize).toFixed(2)),
          memory_delta_mb: parseFloat((results.fs.memory - results.ublda.memory).toFixed(2)),
          time_delta_ms: parseFloat((results.ublda.time - results.fs.time).toFixed(2)),
          time_diff_percent: timeDiff.toFixed(2),
          memory_diff_percent: memoryDiff.toFixed(2),
          storage_diff_percent: storageDiff.toFixed(2)
        }
      };

      console.log('\n📊 Graph Data Output (JSON):\n');
      console.log(JSON.stringify(graphData, null, 2));
    } else {
      // --- Human-Readable Output ---
      console.log('\n📊 --- Performance Comparison ---');
      console.table({
        FS: {
          'Time (ms)': results.fs.time.toFixed(2),
          'Memory (MB)': results.fs.memory.toFixed(2),
          'Storage': formatBytes(results.fs.outputSize)
        },
        UBLDA: {
          'Time (ms)': results.ublda.time.toFixed(2),
          'Memory (MB)': results.ublda.memory.toFixed(2),
          'Storage': formatBytes(results.ublda.outputSize)
        },
        'Relative Δ (%)': {
          'Time (ms)': `${timeDiff.toFixed(2)}%`,
          'Memory (MB)': `${memoryDiff.toFixed(2)}%`,
          'Storage': `${storageDiff.toFixed(2)}%`
        }
      });

      console.log('\n📈 --- Analysis ---');
      console.log(`✔️ Storage Saved: ${(100 - (results.ublda.outputSize / results.fs.outputSize * 100)).toFixed(5)}%`);
      console.log(`✔️ UBLDA is ${(results.fs.outputSize / results.ublda.outputSize).toFixed(1)}x more space-efficient.`);
      console.log(`✔️ UBLDA used ${(results.fs.memory - results.ublda.memory).toFixed(2)} MB lesser memory.`);
      console.log(`⚠️ UBLDA was ${(results.ublda.time - results.fs.time).toFixed(2)} ms slower.`);

      console.log('\n💡 --- Why Use UBLDA? ---');
      console.log(`- 💾 Storage Efficiency: Massive savings via Brotli + deduplication.`);
      console.log(`- 📈 Scalability: Shines on large/repetitive datasets.`);
      console.log(`- ⚡ Speed: Concurrent-safe, optimized for big workflows.`);
      console.log(`- 🔐 Reliability: Robust block-level compression metadata.`);
      console.log(`- 🧩 Best Use Cases: Text-heavy files, logs, archives, backups.`);
    }
    await fs.writeJson('result.json', {
  fs: results.fs,
  ublda: results.ublda
}, { spaces: 2 });
    console.log('\n✅ Results saved to result.json');
  } catch (error) {
    console.error(`❌ Error: ${error.message}`);
    process.exit(1);
  }
}

main();
