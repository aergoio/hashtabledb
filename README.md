[![Build Status](https://github.com/aergoio/hashtabledb/actions/workflows/ci.yml/badge.svg)](https://github.com/aergoio/hashtabledb/actions/workflows/ci.yml)

# HashTableDB

A high-performance embedded key-value database with a hash-table tree index structure

## Overview

HashTableDB is a persistent key-value store designed for high performance and reliability. It uses a combination of append-only logging for data storage and a hash-table tree for indexing

It contains a main hash table. When more than 1 entry use the same slot, the engine will create a new hash-table using the same hash function but with different salt so that on this new page the keys map to different slots.

As each table/page has 818 slots, less pages need to be loaded from disk when reading the value from a key. This means faster reads.

The size of the main hash table can be configured to reach higher speeds, while internal tables use 1 page (4kB)

The iteration of keys is unordered (as in any hash table)

## Features

- **Append-only log file**: Data is written sequentially for optimal write performance
- **Hash-table tree indexing**: Fast key lookups with O(1) average complexity
- **ACID transactions**: Support for atomic operations with commit/rollback
- **Write-Ahead Logging (WAL)**: Ensures durability and crash recovery
- **Configurable write modes**: Balance between performance and durability
- **Efficient page cache**: Minimizes disk I/O with intelligent caching
- **Concurrent access**: Thread-safe operations with appropriate locking

## Architecture

HashTableDB databases consist of three files:

1. **Main file**: Stores all key-value data in an append-only log format
2. **Index file**: Contains the hash-table tree structure for efficient key lookups
3. **WAL file**: Contains flushed index pages, to avoid corruption

### Hash-Table Tree Structure

The index uses a hash-table tree to efficiently locate keys:

- Keys are hashed and distributed across the tree structure
- The tree starts with a root table page
- Table pages act as arrays of pointers, directing lookups to the appropriate sub-pages
- Hybrid pages store a compact hash table that can contain both 
pointers to other pages and key-value data
- As data grows, hybrid pages can also contain pointers to other pages for navigation
- When a hybrid page becomes full, it's converted into a table page

### Page Types

1. **Table Pages**: contain an array of 818 pointers to other pages (4-byte page number + 1-byte sub-page id)
2. **Hybrid Pages**: can store up to 127 sub-pages

A sub-page contains a compact version of a hash table

Each sub-page can contain both pointers to other pages and pointers to key-value data


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

### Mmap reads on the main file

With `"UseMmap": true` the main (append-only) file is mapped read-only shared
and `Get` reads records from the mapping instead of `ReadAt`. Every read still
copies the record out, so the engine can remap the file as it grows (replaced
mappings are kept until `Close` for in-flight readers).

In the default auto mode the mapping uses `MADV_NORMAL` and stays enabled only
while the main file fits in available memory (80% of `MemAvailable` at open,
re-checked when the file grows past the mapping): fault readahead then acts as
free bulk prefetch and cold random reads get dramatically faster. Once the file
outgrows that limit, readahead would evict more than it prefetches, so the
mapping is retired and reads fall back to `ReadAt` — which handles the
larger-than-RAM case far better.

Two options tune this explicitly (either one disables the automatic fit gate):

- `"MmapSize": int64` — fixed mapping reservation in bytes (auto: max(4 GB, 4x file size))
- `"MmapAdvise": "normal" | "random" | "sequential"` — madvise policy for the mapping (auto: `normal`)

For databases much larger than RAM, reads stay on `ReadAt`. There an explicit
`"MainFileAdvise": "random" | "normal"` option can tune the pread path:
`random` disables kernel readahead, which wins when lookups touch a tiny
fraction of the file (measured ~20% faster when ~1% of a DB larger than RAM is
sampled) but loses badly when a meaningful share of the file is read (2.6x
slower at 25% sampling) — leave it unset unless the workload is known to be
sparse.

Benchmark guidance: mmap wins when the whole main file can live in the page
cache (measured 1.4x–20x faster cold random reads depending on kernel
readahead, ~1.5–2.5x faster warm reads, no write-path change). For databases
larger than RAM keep it off (the auto gate does this for you).

## Performance Considerations

- **Write Modes**: Choose between durability and performance
  - `CallerThread_WAL_Sync`: Maximum durability, lowest performance
  - `WorkerThread_WAL`: Good performance and durability (default)
  - `WorkerThread_NoWAL_NoSync`: Maximum performance, lowest durability

- **Cache Size**: Adjust based on available memory and workload
  - Larger cache improves read performance but uses more memory

- **Checkpoint Threshold**: Controls WAL file size before checkpoint
  - Smaller values reduce recovery time but may impact performance

## Implementation Details

- Keys are limited to 2KB
- Values are limited to 128MB
- The database uses a page size of 4KB

## Recovery

The database automatically recovers from crashes by:

1. Reading the main file header
2. Checking for a valid index file
3. Scanning for commit markers in the main file
4. Rebuilding the index if necessary

## Limitations

- Single connection per database file: Only one process can open the database in write mode at a time
- Concurrent access model: Supports one writer thread or multiple reader threads simultaneously

## Performance

HashTableDB is very fast on random reads for a disk-based database engine

### DB < RAM

Benchmark on a machine with 32 GB RAM

| Metric | LevelDB | BadgerDB | RocksDB | ForestDB | HashTableDB |
|--------|---------|----------|---------|----------|-------------|
| Set 11M values | 1m 13.95s | 1m 48.86s | 30.53s | 2m 25.08s | 31.66s |
| 50K txns (10 items each) | 1.04s | 2.31s | 1.37s | 8.55s | 2.17s |
| Space after write | — | 8.94 GB | 8.85 GB | 22.37 GB | 8.58 GB |
| Space after close | 13.19 GB | 13.39 GB | 13.15 GB | 35.66 GB | 13.77 GB |
| Random Reads (cold) | 5m 44.63s | 2m 38.45s | 2m 8.56s | 2m 13.42s | 18.55s |
| Random Reads (warm) | 4m 34.13s | 1m 53.87s | 1m 32.23s | 1m 20.92s | 14.85s |

### DB > RAM

Benchmark on a machine with 3.6 GB RAM

| Metric | LevelDB | BadgerDB | RocksDB | ForestDB | HashTableDB |
|--------|---------|----------|---------|----------|-------------|
| Set 2M values | 2m 5.62s | 3m 26.71s | 1m 5.46s | 2m 4.13s | 1m 2.81s |
| 20K txns (10 items each) | 7.41s | 9.86s | 7.86s | 5.81s | 4.55s |
| Space after write | 4.85 GB | 5.12 GB | 5.15 GB | 10.08 GB | 5.01 GB |
| Space after close | 6.83 GB | 6.96 GB | 6.83 GB | 14.21 GB | 7.21 GB |
| Random Reads (cold) | 7m 5.14s | 6m 4.46s | 7m 58.76s | 3m 23.98s | 2m 46.98s |
| Random Reads (warm) | 6m 3.08s | 5m 59.01s | 7m 24.90s | 2m 46.83s | 2m 4.67s |

Check the full [benchmark results](benchmark-results.md)


## License

Apache 2.0
