[![Build Status](https://github.com/aergoio/hashtabledb/actions/workflows/ci.yml/badge.svg)](https://github.com/aergoio/hashtabledb/actions/workflows/ci.yml)

# HashTableDB

A high-performance embedded key-value database with a [hash-table-tree](https://github.com/kroggen/hash-table-tree) index structure

HashTableDB is particularly fast on random reads (point lookups)

## Overview

HashTableDB is a persistent key-value store designed for high performance and reliability. It uses a combination of append-only logging for data storage and a hash-table-tree for indexing

It contains a main hash table. When more than 1 entry use the same slot, the engine will create a new hash-table using the same hash function but with different salt so that on this new page the keys map to different slots.

As each table/page has 818 slots, less pages need to be loaded from disk when reading the value from a key. This means faster reads.

The size of the main hash table can be configured to reach higher speeds, while internal tables use 1 page (4kB)

The iteration of keys is unordered (as in any hash table)

## Features

- **Append-only log file**: Data is written sequentially for optimal write performance. The index is built from the log
- **Hash-table tree indexing**: Fast key lookups with O(1) average complexity
- **ACID transactions**: Support for atomic operations with commit/rollback
- **Write-Ahead Logging (WAL)**: Ensures durability and crash recovery
- **MVCC page cache**: Minimizes disk I/O by using the page cache instead of reading the WAL
- **Concurrent access**: Many threads can access the database at the same time. 1 writer, multiple readers via MVCC
- **Recovery**: The index file can be rebuilt from the append-only log file

## Architecture

HashTableDB databases consist of 3 files:

1. **Main file**: Stores all key-value data in an append-only log format
2. **Index file**: Contains the hash-table-tree structure for efficient key lookups
3. **WAL file**: Contains flushed index pages, to avoid corruption

### Hash-Table-Tree Structure

The index uses a hash-table-tree to locate keys:

- Keys are hashed and distributed across the tree structure
- The tree starts with a root table page
- Table pages act as arrays of 818 pointers, directing lookups to the appropriate sub-pages
- Hybrid sub-pages store a compact hash table that contain pointers to both other pages and key-value data
- When a hybrid sub-page becomes full, it is converted into a table page

On random reads (point lookups) it has an average of 2-3 disk reads


## Performance

HashTableDB is very fast on random reads (for a disk-based database engine)

### DB < RAM

Benchmark on a machine with 32 GB RAM

| Metric | LevelDB | BadgerDB | RocksDB | ForestDB | HashTableDB |
|--------|---------|----------|---------|----------|-------------|
| Write 11M values | 1m 14s | 1m 49s | 30.53s | 2m 25s | 31.66s |
| 50K txns (10 items each) | 1.04s | 2.31s | 1.37s | 8.55s | 2.17s |
| Space after write | — | 8.94 GB | 8.85 GB | 22.37 GB | 8.58 GB |
| Space after close | 13.19 GB | 13.39 GB | 13.15 GB | 35.66 GB | 13.77 GB |
| Random Reads (cold) | 5m 44s | 2m 38s | 2m 9s | 2m 14s | 18.55s |
| Random Reads (warm) | 4m 34s | 1m 54s | 1m 32s | 1m 21s | 14.85s |

### DB > RAM

Benchmark on a machine with 3.6 GB RAM

| Metric | LevelDB | BadgerDB | RocksDB | ForestDB | HashTableDB |
|--------|---------|----------|---------|----------|-------------|
| Write 2M values | 2m 6s | 3m 27s | 1m 5s | 2m 4s | 1m 3s |
| 20K txns (10 items each) | 7.41s | 9.86s | 7.86s | 5.81s | 4.55s |
| Space after write | 4.85 GB | 5.12 GB | 5.15 GB | 10.08 GB | 5.01 GB |
| Space after close | 6.83 GB | 6.96 GB | 6.83 GB | 14.21 GB | 7.21 GB |
| Random Reads (cold) | 7m 5s | 6m 4s | 7m 59s | 3m 24s | 2m 47s |
| Random Reads (warm) | 6m 3s | 5m 59s | 7m 25s | 2m 47s | 2m 5s |

Check the full [benchmark results](benchmark-results.md)


## Usage

### Basic Operations

```go
// Open or create a database
db, err := hashtabledb.Open("path/to/database")
if err != nil {
    // Handle error
}
defer db.Close()

// Set a key-value pair
err = db.Set([]byte("key"), []byte("value"))

// Get a value
value, err := db.Get([]byte("key"))

// Delete a key
err = db.Delete([]byte("key"))
```

### Transactions

```go
// Begin a transaction
tx, err := db.Begin()
if err != nil {
    // Handle error
}

// Perform operations within the transaction
err = tx.Set([]byte("key1"), []byte("value1"))
err = tx.Set([]byte("key2"), []byte("value2"))

// Commit or rollback
if everythingOk {
    err = tx.Commit()
} else {
    err = tx.Rollback()
}
```

### Configuration Options

```go
options := hashtabledb.Options{
    "ReadOnly": true,                    // Open in read-only mode
    "CacheSizeThreshold": 10000,         // Maximum number of pages in cache
    "DirtyPageThreshold": 5000,          // Maximum dirty pages before flush
    "CheckpointThreshold": 1024 * 1024,  // WAL size before checkpoint (1MB)
    "UseMmap": true,                     // Read values from the main file via mmap
}

db, err := hashtabledb.Open("path/to/database", options)
```

## Performance Considerations

- **Write Modes**: Choose between durability and performance
  - `UseWAL` (default `true`): index page writes go through a WAL,
    checkpointed to the index file in the background
  - `SyncMainFileOnCommit` (default `false`): commits fsync the main file —
    writes that returned success survive a power loss

- **Cache Size**: Adjust based on available memory and workload
  - Larger cache improves read performance but uses more memory

- **Checkpoint Threshold**: Controls WAL file size before checkpoint
  - Smaller values reduce recovery time but may impact performance

mmap wins when the whole main file can live in the page
cache (measured 1.4x–20x faster cold random reads depending on kernel
readahead, ~1.5–2.5x faster warm reads, no write-path change). For databases
larger than RAM keep it off (the auto gate does this for you)


## Limitations

- Keys are limited to 2KB
- Values are limited to 128MB
- Single connection per database file: Only one process can open the database in write mode at a time
- Concurrent access model: Supports one writer thread or multiple reader threads simultaneously


## License

Apache 2.0
