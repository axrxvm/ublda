#!/usr/bin/env node
import { Command } from 'commander';
import ProgressBar from 'progress';
import { storeFile, restoreFile, readFile, writeFile, verifyFile, cleanupBlocks } from '../src/index.js';
import fs from 'fs-extra';

const program = new Command();

program
  .name('ublda')
  .description('Universal Block-Level Data Accelerator')
  .version('1.0.0');

program
  .command('store')
  .argument('<file>', 'file to store')
  .option('--block-size <bytes>', 'block size in bytes', '4096')
  .option('--storage <path>', 'storage directory', './storage')
  .option('--compress', 'compress blocks', false)
  .option('--verbose', 'log progress', false)
  .action(async (file, opts) => {
    try {
      const stats = await fs.stat(file);
      const bar = new ProgressBar('Storing [:bar] :percent :etas', { total: Math.ceil(stats.size / parseInt(opts.blockSize)), width: 30 });
      const result = await storeFile(file, {
        storageDir: opts.storage,
        blockSize: parseInt(opts.blockSize),
        compress: opts.compress,
        verbose: opts.verbose,
      });
      bar.tick(Math.ceil(stats.size / parseInt(opts.blockSize)));
      console.log(`Stored ${file}`);
      console.log(`Manifest: ${result.manifestPath}`);
      console.log(`Blocks: ${result.blockCount}`);
    } catch (error) {
      console.error(`Error storing file: ${error.message}`);
      process.exit(1);
    }
  });

program
  .command('restore')
  .argument('<manifest>', 'manifest path')
  .argument('<output>', 'output file path')
  .option('--storage <path>', 'storage directory', './storage')
  .option('--verify', 'verify block hashes', false)
  .option('--verbose', 'log progress', false)
  .action(async (manifest, output, opts) => {
    try {
      const { hashes } = await fs.readJson(manifest);
      const bar = new ProgressBar('Restoring [:bar] :percent :etas', { total: hashes.length, width: 30 });
      await restoreFile(manifest, output, {
        storageDir: opts.storage,
        verifyHashes: opts.verify,
        verbose: opts.verbose,
      });
      bar.tick(hashes.length);
      console.log(`Restored to ${output}`);
    } catch (error) {
      console.error(`Error restoring file: ${error.message}`);
      process.exit(1);
    }
  });

program
  .command('read')
  .argument('<manifest>', 'manifest path')
  .option('--storage <path>', 'storage directory', './storage')
  .option('--verify', 'verify block hashes', false)
  .option('--verbose', 'log progress', false)
  .option('--stream', 'output as stream to stdout', false)
  .action(async (manifest, opts) => {
    try {
      const { hashes } = await fs.readJson(manifest);
      const bar = new ProgressBar('Reading [:bar] :percent :etas', { total: hashes.length, width: 30 });
      const result = await readFile(manifest, {
        storageDir: opts.storage,
        verifyHashes: opts.verify,
        verbose: opts.verbose,
        stream: opts.stream,
      });
      if (opts.stream) {
        result.pipe(process.stdout);
      } else {
        process.stdout.write(result);
      }
      bar.tick(hashes.length);
      console.log(`Read ${hashes.length} blocks`);
    } catch (error) {
      console.error(`Error reading file: ${error.message}`);
      process.exit(1);
    }
  });

program
  .command('write')
  .argument('<filename>', 'logical filename')
  .option('--storage <path>', 'storage directory', './storage')
  .option('--block-size <bytes>', 'block size in bytes', '1048576')
  .option('--compress', 'compress blocks', false)
  .option('--verbose', 'log progress', false)
  .action(async (filename, opts) => {
    try {
      const buffer = await new Promise((resolve, reject) => {
        const chunks = [];
        process.stdin.on('data', chunk => chunks.push(chunk));
        process.stdin.on('end', () => resolve(Buffer.concat(chunks)));
        process.stdin.on('error', reject);
      });
      const bar = new ProgressBar('Writing [:bar] :percent :etas', { total: Math.ceil(buffer.length / parseInt(opts.blockSize)), width: 30 });
      const result = await writeFile(filename, buffer, {
        storageDir: opts.storage,
        blockSize: parseInt(opts.blockSize),
        compress: opts.compress,
        verbose: opts.verbose,
      });
      bar.tick(Math.ceil(buffer.length / parseInt(opts.blockSize)));
      console.log(`Wrote ${filename}`);
      console.log(`Manifest: ${result.manifestPath}`);
      console.log(`Blocks: ${result.blockCount}`);
      console.log(`Total Size: ${result.totalSize} bytes`);
    } catch (error) {
      console.error(`Error writing file: ${error.message}`);
      process.exit(1);
    }
  });

program
  .command('verify')
  .argument('<file>', 'file to verify')
  .argument('<manifest>', 'manifest path')
  .option('--storage <path>', 'storage directory', './storage')
  .option('--verbose', 'log progress', false)
  .action(async (file, manifest, opts) => {
    try {
      const { hashes } = await fs.readJson(manifest);
      const bar = new ProgressBar('Verifying [:bar] :percent :etas', { total: hashes.length, width: 30 });
      await verifyFile(file, manifest, { storageDir: opts.storage, verbose: opts.verbose });
      bar.tick(hashes.length);
      console.log(`Verification passed for ${file}`);
    } catch (error) {
      console.error(`Error verifying file: ${error.message}`);
      process.exit(1);
    }
  });

program
  .command('cleanup')
  .option('--storage <path>', 'storage directory', './storage')
  .option('--verbose', 'log progress', false)
  .action(async (opts) => {
    try {
      const bar = new ProgressBar('Cleaning [:bar] :percent :etas', { total: 100, width: 30 });
      const deleted = await cleanupBlocks(opts.storage, { verbose: opts.verbose });
      bar.tick(100);
      console.log(`Cleaned ${deleted} unused blocks`);
    } catch (error) {
      console.error(`Error cleaning blocks: ${error.message}`);
      process.exit(1);
    }
  });

program.parse();