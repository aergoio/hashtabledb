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
}

db, err := hashtabledb.Open("path/to/database", options)
```

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

| Metric | LevelDB | BadgerDB | ForestDB | HashTableDB |
|--------|---------|----------|----------|-------------|
| Set 2M values | 2m 44.45s | 13.81s | 18.95s | 8.30s |
| 20K txns (10 items each) | 1m 0.09s | 1.32s | 2.78s | 1.80s |
| Space after write | 1052.08 MB | 2002.38 MB | 1715.76 MB | 1501.09 MB |
| Space after close | 1158.78 MB | 1203.11 MB | 2223.16 MB | 1899.50 MB |
| Read 2M values (fresh) | 1m 26.87s | 35.14s | 17.21s | 12.80s |
| Read 2M values (cold) | 1m 34.46s | 38.36s | 16.84s | 11.33s |

Benchmark writting 2 million records (key: 33 random bytes, value: 750 random bytes) in a single transaction and then reading them in non-sequential order. Also insertion using 20 thousand transactions

HashTableDB is very fast on reads for a disk-based database engine. It is more than 3.3x faster than BadgerDB on reads

Here is a comparison with LMDB:

| Metric | HashTableDB | LMDB |
|--------|-------------|------|
| Set 2M values | 8.30s | 29.32s |
| 20K txns (10 items each) | 1.80s | 4m 9.68s |
| Space after write | 1501.09 MB | 2352.35 MB |
| Space after close | 1899.31 MB | 2586.98 MB |
| Read 2M values (fresh) | 12.80s | 4.88s |
| Read 2M values (cold) | 11.33s | 5.58s |

LMDB is the fastest on reads, but way slower on writes and using more disk space

## License

Apache 2.0
