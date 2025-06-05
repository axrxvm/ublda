# UBLDA - Universal Block-Level Data Accelerator

**Made by: Aarav Mehta**

## Introduction

UBLDA (Universal Block-Level Data Accelerator) is a Node.js-based tool and library designed for efficient file storage through block-level deduplication. It splits files into smaller blocks, hashes them, and stores only the unique blocks. This approach, known as content-addressable storage, significantly reduces storage space requirements when dealing with redundant data or large files with minor differences. UBLDA also offers optional compression for further space savings and can be used both as a command-line interface (CLI) tool and as a library in your JavaScript/TypeScript applications.

## Core Logic Explained

The core functionality of UBLDA revolves around a few key concepts:

1.  **File Splitting:** When you store a file, UBLDA divides it into fixed-size blocks (the block size is configurable).
2.  **Hashing and Content-Addressable Storage:** Each block is then hashed using a cryptographic hash function. This hash serves as the block's unique identifier. Blocks are typically stored in a directory structure where the filename is derived from or is the hash itself.
3.  **Deduplication:** Before storing a new block, UBLDA checks if a block with the same hash already exists in the storage. If it does, the new block is not stored again; instead, the system simply references the existing block. This is the essence of deduplication.
4.  **Optional Compression:** UBLDA can compress blocks (using Brotli or Deflate) before hashing and storing them. This can lead to further reductions in storage space, especially for text-based or uncompressed file formats. A heuristic is used to determine if compression is likely to be beneficial.
5.  **Manifest Files:** For each file processed, UBLDA creates a JSON manifest file (e.g., `original_filename.manifest.json`). This manifest acts as a blueprint for reconstructing the original file. It contains:
    *   An ordered list of the hashes of all the blocks that make up the file.
    *   Information about whether each block was compressed and which algorithm was used.
    *   Essential metadata such as the original file size, the block size used during storage, compression settings, the original filename, and the creation timestamp.

## Why Use UBLDA?

UBLDA offers several advantages for managing and storing file data:

*   **Storage Space Savings:** Deduplication ensures that identical data blocks are stored only once, which can lead to significant reductions in storage consumption, especially when storing multiple versions of files, backups, or large datasets with repetitive content.
*   **Efficient Data Transfer:** When transferring files that have been processed with UBLDA, you might only need to send the blocks that the recipient doesn't already have, potentially speeding up data exchange.
*   **Data Integrity:** The use of cryptographic hashes for block identification helps in verifying data integrity. If a stored block is corrupted, its hash will no longer match, making it easy to detect errors. The `verify` command specifically checks this.
*   **Flexibility:** UBLDA can be used as a standalone CLI tool for quick operations or integrated as a library into Node.js applications for more complex workflows.
*   **Configurable:** Options like block size and compression can be tuned to suit specific needs.

## Features

*   **Store Files:** Splits files into blocks, deduplicates, optionally compresses, and saves them along with a manifest.
*   **Restore Files:** Reconstructs original files from their manifests and stored blocks.
*   **Read Files:** Reads file data directly from manifests and blocks into a Buffer or as a Stream.
*   **Write Data:** Takes data from stdin, processes it like `store`, and creates a manifest.
*   **Verify Files:** Checks the integrity of a restored file against its manifest and the stored blocks.
*   **Cleanup:** Removes orphaned blocks from storage that are no longer referenced by any manifest (available via the `ublda cleanup` CLI command). Programmatic access is also possible.
*   **Command-Line Interface (CLI):** Easy-to-use commands for all core operations.
*   **Programmatic API:** Importable JavaScript modules for use in your own applications.
*   **Optional Compression:** Supports Brotli and Deflate compression algorithms.

## Installation

### Using npm (recommended for library usage and general CLI use):
If you have Node.js and npm installed, you can install UBLDA globally for CLI access or locally for your project:
```bash
# For global CLI access
npm install -g ublda
ublda --help

# For local project library usage
npm install ublda
```

### From Source (for development or specific versions):
```bash
git clone https://github.com/your-username/ublda.git # Replace with actual repo URL if available
cd ublda
npm install # Install dependencies
npm link    # To make 'ublda' command available globally from this source
# Or run directly: node bin/cli.js --help
```
*(Note: Update the clone URL if the project is hosted on a platform like GitHub.)*

## Basic CLI Usage

Here are a few examples of how to use the UBLDA CLI:

*   **Store a file:**
    ```bash
    ublda store ./myfile.txt --storage ./my_ublda_storage --compress
    ```
    This will store `myfile.txt` into the `./my_ublda_storage` directory, enabling compression. A manifest file (e.g., `myfile.txt.manifest.json`) will be created in the `manifests` subdirectory of your storage.

*   **Restore a file:**
    ```bash
    ublda restore ./my_ublda_storage/manifests/myfile.txt.manifest.json ./restored_myfile.txt --storage ./my_ublda_storage
    ```
    This will restore the file using its manifest to `restored_myfile.txt`.

*   **Read a file to stdout:**
    ```bash
    ublda read ./my_ublda_storage/manifests/myfile.txt.manifest.json --storage ./my_ublda_storage > output.txt
    ```

*   **Verify a file:**
    ```bash
    ublda verify ./restored_myfile.txt ./my_ublda_storage/manifests/myfile.txt.manifest.json --storage ./my_ublda_storage
    ```

For more commands and options, use `ublda --help` or refer to [USAGE.md](./USAGE.md).

## Programmatic Usage (Brief Example)

UBLDA can be used as a library in your Node.js projects. Install it via npm: `npm install ublda`.

Here's a quick peek:

```javascript
import { storeFile, restoreFile } from 'ublda'; // ESM
// For CommonJS: const { storeFile, restoreFile } = require('ublda');
import path from 'path'; // Standard Node.js module
import fs from 'fs-extra'; // Popular file system utility

async function example() {
  const storageDir = path.join(process.cwd(), 'my_ublda_storage_prog');
  const originalFilePath = path.join(process.cwd(), 'my_sample_file.txt');
  const restoredFilePath = path.join(process.cwd(), 'my_sample_file_restored.txt');

  try {
    // Ensure storage directory and necessary subdirectories exist
    // Note: storeFile and other functions typically create 'blocks' and 'manifests' if they don't exist.
    await fs.ensureDir(storageDir);
    await fs.writeFile(originalFilePath, 'Hello UBLDA! This is a test file. Repeating content helps show deduplication.');

    console.log('Storing file...');
    const { manifestPath } = await storeFile(originalFilePath, {
      storageDir,
      compress: true,
      verbose: false // Set to true for more logs
    });
    console.log(`File stored. Manifest at: ${manifestPath}`);

    console.log('Restoring file...');
    await restoreFile(manifestPath, restoredFilePath, {
      storageDir,
      verbose: false // Set to true for more logs
    });
    console.log(`File restored to: ${restoredFilePath}`);

    // Verification
    const originalContent = await fs.readFile(originalFilePath, 'utf-8');
    const restoredContent = await fs.readFile(restoredFilePath, 'utf-8');
    if (originalContent === restoredContent) {
      console.log('Verification successful: Original and restored content match.');
    } else {
      console.error('Verification failed: Content mismatch.');
    }

  } catch (error) {
    console.error('UBLDA programmatic example failed:', error);
  } finally {
    // Clean up
    await fs.remove(originalFilePath).catch(e => console.error('Cleanup error originalFilePath:', e));
    await fs.remove(restoredFilePath).catch(e => console.error('Cleanup error restoredFilePath:', e));
    await fs.remove(storageDir).catch(e => console.error('Cleanup error storageDir:', e)); // Be cautious with rm -rf style operations
  }
}

example();
```
For detailed information on all exported functions, parameters, and advanced usage, please refer to the [USAGE.md](./USAGE.md) file (which you will create in the next step).

## License

This project is licensed under the MIT License. See the [LICENSE](./LICENSE) file for details.
