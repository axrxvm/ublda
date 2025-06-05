# UBLDA In-Depth Usage Guide

## Introduction

This document provides comprehensive documentation for using UBLDA (Universal Block-Level Data Accelerator), both as a Command-Line Interface (CLI) tool and as a JavaScript library in your applications.

For a general overview, features, and basic examples, please see the [README.md](./README.md).

## CLI Tool Usage

The UBLDA CLI allows you to perform storage, restoration, and management operations directly from your terminal.

### General Syntax

```bash
ublda [command] [arguments...] [options...]
```

To see all available commands and global options:
```bash
ublda --help
```
To see options for a specific command:
```bash
ublda [command] --help
```

### Commands

#### 1. `store`

Stores a file by splitting it into blocks, deduplicating, optionally compressing, and creating a manifest.

**Syntax:**
```bash
ublda store <file> [options]
```

**Arguments:**
*   `<file>`: (Required) Path to the file you want to store.

**Options:**
*   `--block-size <bytes>`: Specifies the size of blocks in bytes.
    *   Default: `4096` (This is the default for the CLI `store` command).
*   `--storage <path>`: Specifies the directory where blocks and manifests will be stored.
    *   Default: `./storage`
*   `--compress`: Enables compression of blocks. If not set, blocks are stored uncompressed.
    *   Default: `false` (Store uncompressed unless this flag is present)
*   `--verbose`: Enables detailed logging of progress and operations.
    *   Default: `false`

**Example:**
```bash
# Store 'document.pdf' with 8KB blocks, compression, into './backup_storage'
ublda store ./document.pdf --block-size 8192 --storage ./backup_storage --compress --verbose
```
On success, it will output the path to the manifest file and the number of blocks created.

#### 2. `restore`

Restores a file from its manifest and the stored blocks.

**Syntax:**
```bash
ublda restore <manifest> <output> [options]
```

**Arguments:**
*   `<manifest>`: (Required) Path to the manifest file (e.g., `document.pdf.manifest.json`).
*   `<output>`: (Required) Path where the restored file will be saved.

**Options:**
*   `--storage <path>`: Specifies the directory where blocks are stored. This must be the same storage directory used when the file was stored.
    *   Default: `./storage`
*   `--verbose`: Enables detailed logging.
    *   Default: `false`
*   `--verify`: (Note: This option is available in the underlying `restoreFile` library function but not explicitly listed as an option for the `restore` command in `bin/cli.js`'s help output. It might be an internal pass-through or an undocumented feature for the CLI.) When used programmatically, it verifies block hashes during restoration.
    *   Default for CLI: Not explicitly available. Programmatic default: `false`.


**Example:**
```bash
ublda restore ./backup_storage/manifests/document.pdf.manifest.json ./retrieved_document.pdf --storage ./backup_storage
```

#### 3. `read`

Reads a file from its manifest and blocks, then outputs its content to standard output (stdout).

**Syntax:**
```bash
ublda read <manifest> [options]
```

**Arguments:**
*   `<manifest>`: (Required) Path to the manifest file.

**Options:**
*   `--storage <path>`: Specifies the storage directory.
    *   Default: `./storage`
*   `--stream`: Outputs the file content as a raw stream to stdout. Useful for piping to other commands that can process streams.
    *   Default: `false` (Outputs the full content after buffering).
*   `--verbose`: Enables detailed logging.
    *   Default: `false`
*   `--verify`: (Similar to `restore`, this option is in the underlying library function `readFile` but not explicitly in CLI `read` command's help.) Programmatic default: `false`.

**Example:**
```bash
# Read 'archive.zip.manifest.json' and save its content to 'archive_copy.zip'
ublda read ./my_storage/manifests/archive.zip.manifest.json --storage ./my_storage > archive_copy.zip

# Stream content to another process
ublda read ./my_storage/manifests/log.txt.manifest.json --storage ./my_storage --stream | grep "ERROR"
```

#### 4. `write`

Reads data from standard input (stdin), stores it as blocks (with deduplication and optional compression), and creates a manifest for the given logical filename.

**Syntax:**
```bash
ublda write <filename> [options]
```

**Arguments:**
*   `<filename>`: (Required) The logical filename to be associated with the data from stdin. This name will be used in the manifest file (e.g., `<filename>.manifest.json`).

**Options:**
*   `--storage <path>`: Specifies the storage directory.
    *   Default: `./storage`
*   `--block-size <bytes>`: Specifies the block size.
    *   Default: `1048576` (1MB, for the CLI `write` command).
*   `--compress`: Enables compression.
    *   Default: `false`
*   `--verbose`: Enables detailed logging.
    *   Default: `false`

**Example:**
```bash
# Create a tar archive and pipe it directly to ublda write
tar -czf - ./my_project_files | ublda write my_project_backup.tar.gz --storage ./archive_storage --compress
```
This command will read the tarball data from stdin, store it, and create `my_project_backup.tar.gz.manifest.json`.

#### 5. `verify`

Verifies the integrity of a local file against its manifest and the blocks stored in the UBLDA storage. It checks that the file can be reconstructed to match the manifest and that the stored blocks themselves are not corrupted (by re-hashing them).

**Syntax:**
```bash
ublda verify <file> <manifest> [options]
```

**Arguments:**
*   `<file>`: (Required) Path to the local file you want to verify.
*   `<manifest>`: (Required) Path to the manifest file corresponding to the local file.

**Options:**
*   `--storage <path>`: Specifies the storage directory.
    *   Default: `./storage`
*   `--verbose`: Enables detailed logging.
    *   Default: `false`

**Example:**
```bash
ublda verify ./retrieved_document.pdf ./backup_storage/manifests/document.pdf.manifest.json --storage ./backup_storage
```
If verification passes, it will print a success message. If it fails, it will output an error detailing the mismatch or corruption.

#### 6. `cleanup`

Scans the storage directory for orphaned blocks (blocks that exist in `storage/blocks` but are not referenced by any manifest in `storage/manifests`) and removes them. This helps reclaim storage space.

**Syntax:**
```bash
ublda cleanup [options]
```

**Options:**
*   `--storage <path>`: Specifies the storage directory to clean.
    *   Default: `./storage`
*   `--verbose`: Enables detailed logging, showing which blocks are being checked and deleted.
    *   Default: `false`

**Example:**
```bash
ublda cleanup --storage ./backup_storage --verbose
```
Outputs the number of unused blocks deleted.

## Programmatic Usage (as a Library)

UBLDA can be integrated into your Node.js applications to leverage its functionalities programmatically.

### Installation

Install UBLDA as a dependency in your project:
```bash
npm install ublda
```

### Importing

```javascript
// ES Module syntax
import {
  storeFile,
  restoreFile,
  readFile,
  writeFile,
  verifyFile,
  cleanupBlocks // cleanupBlocks is the programmatic equivalent of 'ublda cleanup'
} from 'ublda';

// CommonJS syntax (if your project uses it)
// const { storeFile, restoreFile, ... } = require('ublda');
```
You will also likely need `fs-extra` and `path`:
```javascript
import fs from 'fs-extra';
import path from 'path';
```

### Core Functions

All functions are asynchronous and return Promises.

#### 1. `storeFile(filePath, options)`

Stores a physical file.

*   `filePath`: `String` - Absolute or relative path to the file to be stored.
*   `options`: `Object` (Optional)
    *   `storageDir`: `String` - Directory for storing blocks and manifests. Subdirectories `blocks` and `manifests` will be created if they don't exist.
        *   Default: `'./storage'`
    *   `blockSize`: `Number` - Size of blocks in bytes.
        *   Default: `8 * 1024 * 1024` (8MB)
    *   `compress`: `Boolean` - Whether to attempt compression.
        *   Default: `true`
    *   `compressionAlgorithm`: `String` - Algorithm to use if `compress` is true.
        *   Values: `'brotli'`, `'deflate'`, `'none'`
        *   Default: `'brotli'`
    *   `verbose`: `Boolean` - Enable detailed console logging.
        *   Default: `false`
*   **Returns:** `Promise<{ manifestPath: String, blockCount: Number }>`
    *   `manifestPath`: Path to the created manifest file.
    *   `blockCount`: Number of unique blocks created/referenced for this file.

**Example:**
```javascript
import { storeFile } from 'ublda';
import fs from 'fs-extra';
import path from 'path';

async function runStoreExample() {
  const testFile = path.join(process.cwd(), 'my_document.txt');
  const storage = path.join(process.cwd(), 'my_app_storage_store');

  try {
    await fs.writeFile(testFile, 'This is content for UBLDA storeFile example. Repeat content for deduplication benefits.');
    await fs.ensureDir(storage); // Ensure base storage dir exists

    const result = await storeFile(testFile, {
      storageDir: storage,
      blockSize: 1024 * 1024, // 1MB blocks
      compress: true,
      verbose: false
    });
    console.log(`File stored successfully! Manifest: ${result.manifestPath}, Blocks: ${result.blockCount}`);
  } catch (error) {
    console.error('Error storing file:', error);
  } finally {
    await fs.remove(testFile).catch(console.error);
    await fs.remove(storage).catch(console.error); // Clean up
  }
}
runStoreExample();
```

#### 2. `restoreFile(manifestPath, outputPath, options)`

Restores a file from its manifest.

*   `manifestPath`: `String` - Path to the manifest file.
*   `outputPath`: `String` - Path where the restored file will be written.
*   `options`: `Object` (Optional)
    *   `storageDir`: `String` - Directory where blocks are stored.
        *   Default: `'./storage'`
    *   `verbose`: `Boolean` - Enable detailed console logging.
        *   Default: `false`
*   **Returns:** `Promise<void>` - Resolves when restoration is complete.

**Example:**
```javascript
// This example assumes a file has already been stored and its manifestPath is known.
// For a self-contained example, you'd call storeFile first.
import { restoreFile } from 'ublda';
import fs from 'fs-extra';
import path from 'path';

async function runRestoreExample(manifestPathToRestore, storageLocation) {
  // manifestPathToRestore would come from a previous storeFile operation.
  // e.g., manifestPathToRestore = './my_app_storage_store/manifests/my_document.txt.manifest.json'
  const restoredOutput = path.join(process.cwd(), 'my_document_restored.txt');

  try {
    await restoreFile(manifestPathToRestore, restoredOutput, {
      storageDir: storageLocation,
      verbose: false
    });
    console.log(`File restored successfully to: ${restoredOutput}`);
    // TODO: Add verification logic if needed (e.g., compare with original)
  } catch (error) {
    console.error('Error restoring file:', error);
  } finally {
    await fs.remove(restoredOutput).catch(console.error);
  }
}
// Example call (ensure manifest and storage exist from a previous 'store' operation):
// runRestoreExample(
//   './my_app_storage_store/manifests/my_document.txt.manifest.json',
//   './my_app_storage_store'
// );
```

#### 3. `readFile(manifestPath, options)`

Reads a file defined by a manifest and returns its content.

*   `manifestPath`: `String` - Path to the manifest file.
*   `options`: `Object` (Optional)
    *   `storageDir`: `String` - Directory where blocks are stored.
        *   Default: `'./storage'`
    *   `verbose`: `Boolean` - Enable detailed console logging.
        *   Default: `false`
    *   `stream`: `Boolean` - If `true`, returns a Readable Stream. If `false`, returns a Buffer.
        *   Default: `false` (returns Buffer)
*   **Returns:** `Promise<Buffer|ReadableStream>` - Buffer with file content or a ReadableStream.

**Example (Buffer):**
```javascript
// Assumes a manifest exists from a prior 'store' operation.
import { readFile } from 'ublda';
import fs from 'fs-extra';
import path from 'path';

async function runReadBufferExample(manifestPathToRead, storageLocation) {
  const tempOutputPath = path.join(process.cwd(), 'read_output_buffer.tmp');
  try {
    const fileBuffer = await readFile(manifestPathToRead, {
      storageDir: storageLocation,
      verbose: false
    });
    console.log(`File content read successfully. Size: ${fileBuffer.length} bytes.`);
    // console.log(fileBuffer.toString('utf-8')); // If it's a text file
    await fs.writeFile(tempOutputPath, fileBuffer); // Example: save buffer to a temporary file
    console.log(`Buffer saved to ${tempOutputPath}`);
  } catch (error) {
    console.error('Error reading file (buffer):', error);
  } finally {
     await fs.remove(tempOutputPath).catch(console.error);
  }
}
// runReadBufferExample(
//   './my_app_storage_store/manifests/my_document.txt.manifest.json',
//   './my_app_storage_store'
// );
```

**Example (Stream):**
```javascript
// Assumes a manifest exists.
import { readFile } from 'ublda';
import fs from 'fs'; // Using Node's core fs for createWriteStream
import fse from 'fs-extra'; // For cleanup
import path from 'path';

async function runReadStreamExample(manifestPathToRead, storageLocation) {
  const outputPath = path.join(process.cwd(), 'read_output_stream.tmp');
  try {
    const readableStream = await readFile(manifestPathToRead, {
      storageDir: storageLocation,
      stream: true,
      verbose: false
    });

    const writableStream = fs.createWriteStream(outputPath);
    readableStream.pipe(writableStream);

    await new Promise((resolve, reject) => {
      writableStream.on('finish', resolve);
      writableStream.on('error', reject);
      readableStream.on('error', reject);
    });
    console.log(`File content streamed successfully to: ${outputPath}`);
  } catch (error) {
    console.error('Error reading file (stream):', error);
  } finally {
    await fse.remove(outputPath).catch(console.error);
  }
}
// runReadStreamExample(
//   './my_app_storage_store/manifests/my_document.txt.manifest.json',
//   './my_app_storage_store'
// );
```

#### 4. `writeFile(filename, buffer, options)`

Stores data from a Buffer.

*   `filename`: `String` - Logical filename to associate with the data (used for the manifest name).
*   `buffer`: `Buffer` - The data to store.
*   `options`: `Object` (Optional)
    *   `storageDir`: `String` - Directory for storing blocks and manifests.
        *   Default: `'./storage'`
    *   `blockSize`: `Number` - Size of blocks in bytes.
        *   Default: `8 * 1024 * 1024` (8MB)
    *   `compress`: `Boolean` - Whether to attempt compression.
        *   Default: `true`
    *   `compressionAlgorithm`: `String` - Algorithm if `compress` is true.
        *   Values: `'brotli'`, `'deflate'`, `'none'`
        *   Default: `'brotli'`
    *   `verbose`: `Boolean` - Enable detailed console logging.
        *   Default: `false`
*   **Returns:** `Promise<{ manifestPath: String, blockCount: Number, totalSize: Number }>`
    *   `manifestPath`: Path to the created manifest file.
    *   `blockCount`: Number of unique blocks.
    *   `totalSize`: Original size of the buffer.

**Example:**
```javascript
import { writeFile } from 'ublda';
import fs from 'fs-extra';
import path from 'path';

async function runWriteBufferExample() {
  const myData = Buffer.from('Some data generated directly in my application for writeFile example.');
  const logicalName = 'app_generated_data.txt';
  const storage = path.join(process.cwd(), 'my_buffer_storage_write');

  try {
    await fs.ensureDir(storage);
    const result = await writeFile(logicalName, myData, {
      storageDir: storage,
      compress: false,
      verbose: false
    });
    console.log(`Buffer stored: Manifest: ${result.manifestPath}, Blocks: ${result.blockCount}`);
  } catch (error) {
    console.error('Error writing buffer:', error);
  } finally {
    await fs.remove(storage).catch(console.error);
  }
}
runWriteBufferExample();
```

#### 5. `verifyFile(filePath, manifestPath, options)`

Verifies a local file against its manifest and stored blocks.

*   `filePath`: `String` - Path to the local file on disk.
*   `manifestPath`: `String` - Path to the UBLDA manifest file for this file.
*   `options`: `Object` (Optional)
    *   `storageDir`: `String` - Directory where UBLDA blocks are stored.
        *   Default: `'./storage'`
    *   `verbose`: `Boolean` - Enable detailed console logging.
        *   Default: `false`
*   **Returns:** `Promise<boolean>` - Resolves with `true` if verification is successful. Rejects with an Error if verification fails (error message will contain details).

**Example:**
```javascript
// Assumes 'my_document_restored.txt' was created by restoreFile
// and 'manifestPathToRestore' points to its manifest.
import { verifyFile } from 'ublda';
// You would typically run this after a restoreFile operation.

async function runVerifyExample(fileToVerify, manifestForFile, storageLocation) {
  try {
    const isValid = await verifyFile(fileToVerify, manifestForFile, {
      storageDir: storageLocation,
      verbose: false
    });
    if (isValid) {
      console.log(`Verification successful for: ${fileToVerify}`);
    } else {
      // This else case might not be reached if verifyFile rejects on failure.
      // The catch block is more typical for handling verification failures.
      console.log(`Verification reported false for: ${fileToVerify}`);
    }
  } catch (error) {
    console.error(`Verification failed for ${fileToVerify}: ${error.message}`);
  }
}
// Example call (ensure file, manifest, and storage exist):
// runVerifyExample(
//   './my_document_restored.txt', // Path to the file on disk
//   './my_app_storage_store/manifests/my_document.txt.manifest.json', // Path to its manifest
//   './my_app_storage_store' // Storage directory
// );
```

#### 6. `cleanupBlocks(storageDirPath, options)`

Removes unused blocks from the specified storage directory.

*   `storageDirPath`: `String` - (Required) The path to the UBLDA storage directory to clean (e.g., `./my_storage`).
*   `options`: `Object` (Optional)
    *   `verbose`: `Boolean` - Enable detailed console logging.
        *   Default: `false`
*   **Returns:** `Promise<number>` - Resolves with the count of deleted blocks.

**Example:**
```javascript
import { cleanupBlocks, storeFile, writeFile } from 'ublda'; // Assuming these exports exist
import fs from 'fs-extra';
import path from 'path';

async function runCleanupExample() {
  const storageLocation = path.join(process.cwd(), 'my_app_storage_cleanup');
  const tempFile = path.join(process.cwd(), 'temp_for_cleanup.txt');

  try {
    await fs.ensureDir(storageLocation);
    await fs.writeFile(tempFile, 'Data for a file that will have its manifest deleted.');

    // Store a file to create some blocks and a manifest
    const { manifestPath } = await storeFile(tempFile, { storageDir: storageLocation });
    console.log(`Stored file with manifest: ${manifestPath}`);

    // Now, simulate an orphaned block scenario by deleting the manifest
    await fs.remove(manifestPath);
    console.log(`Deleted manifest: ${manifestPath} to simulate orphaned blocks.`);

    // Add another block via writeFile that won't be orphaned
    await writeFile('persistent_data.dat', Buffer.from('this block should remain'), {storageDir: storageLocation});


    const deletedCount = await cleanupBlocks(storageLocation, { verbose: true });
    console.log(`Cleanup complete. Deleted ${deletedCount} orphaned blocks from ${storageLocation}.`);

  } catch (error) {
    console.error(`Error during cleanup example for ${storageLocation}:`, error);
  } finally {
    await fs.remove(tempFile).catch(console.error);
    await fs.remove(storageLocation).catch(console.error);
  }
}
runCleanupExample();
```

### Important Considerations for Programmatic Use

*   **Error Handling:** All functions return Promises. Always use `.then().catch()` or `async/await` with `try...catch` blocks to handle potential errors (e.g., file not found, invalid manifest, I/O errors).
*   **Storage Directory Management:** While `storeFile` and `writeFile` create their necessary subdirectories (`blocks`, `manifests`) within the provided `storageDir` if they don't exist, your application should manage the lifecycle (creation, cleanup) of the base `storageDir` itself.
*   **File Paths:** Use `path.join()` and consider `process.cwd()` or absolute paths for robustness, especially when dealing with files and directories.
*   **Concurrency:** The library uses `p-limit` internally for some operations. Be mindful of how many UBLDA operations you run concurrently at the application level if dealing with a very large number of files to avoid exhausting resources.

This completes the detailed usage guide.
