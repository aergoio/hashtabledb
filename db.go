package hashtabledb

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aergoio/hashtabledb/varint"
)

const (
	// Page size (4KB)
	PageSize = 4096
	// Magic strings for database identification (6 bytes)
	MainFileMagicString  string = "HT_LOG"
	IndexFileMagicString string = "HT_IDX"
	// Database version (2 bytes as a string)
	VersionString string = "\x00\x01"
	// Maximum key length
	MaxKeyLength = 2048
	// Maximum value length
	MaxValueLength = 2 << 26 // 128MB
	// Alignment for non-page content
	ContentAlignment = 8

	// Table page header size
	TableHeaderSize = 6  // Checksum(4) + Type(1) + Salt(1)
	// Hybrid page header size
	HybridHeaderSize = 8  // Checksum(4) + Type(1) + NumSubPages(1) + ContentSize(2)
	// Table page entries
	TableEntries = 818   // 818 entries of 5 bytes each -> 4090 bytes
	// Size of each table entry
	TableEntrySize = 5   // 5 bytes for pointer/offset

	// Hybrid sub-page header size
	HybridSubPageHeaderSize = 4 // ID(1) + Salt(1) + Size(2)
	// Minimum free space in bytes required to add a hybrid page to the free space array
	MIN_FREE_SPACE = 64

	// Main index configuration
	DefaultMainIndexPages = 256 // Default number of pages in main index (1MB)
	InitialSalt = 0             // Initial salt for main index pages
)

// Content types
const (
	ContentTypeData   = 'D' // Data content type
	ContentTypeCommit = 'C' // Commit marker type
	ContentTypeTable  = 'T' // Table page type
	ContentTypeHybrid = 'H' // Hybrid page type
)

// Lock types
const (
	LockNone    = 0 // No locking
	LockShared  = 1 // Shared lock (read-only)
	LockExclusive = 2 // Exclusive lock (read-write)
)

// Write modes
const (
	CallerThread_WAL_Sync      = "CallerThread_WAL_Sync"     // write to WAL on the caller thread, checkpoint on the background thread
	CallerThread_WAL_NoSync    = "CallerThread_WAL_NoSync"   // write to WAL on the caller thread, checkpoint on the background thread
	WorkerThread_WAL           = "WorkerThread_WAL"          // write to WAL and checkpoint on the background thread
	WorkerThread_NoWAL         = "WorkerThread_NoWAL"        // write directly to file on the background thread
	WorkerThread_NoWAL_NoSync  = "WorkerThread_NoWAL_NoSync" // write directly to file on the background thread
)

// Commit modes
const (
	CallerThread = 1 // Commit on the caller thread
	WorkerThread = 0 // Commit on a background worker thread
)

// Sync modes
const (
	SyncOn  = 1 // Sync after writes
	SyncOff = 0 // Don't sync after writes
)

// Free space tracking
const (
	MaxFreeSpaceEntries = 500 // Maximum number of free space entries in the array
)

// Value cache configuration
const (
	DefaultValueCacheThreshold = 8 * 1024 * 1024 // Default maximum memory in bytes for value cache (8MB)
)

// FreeSpaceEntry represents an entry in the free space array
type FreeSpaceEntry struct {
	PageNumber uint32 // Page number of the hybrid page
	FreeSpace  uint16 // Amount of free space in bytes
}

// cacheBucket represents a bucket in the page cache with its own mutex
type cacheBucket struct {
    mutex sync.RWMutex
    pages map[uint32]*Page  // Map of page numbers to pages
}

// valueCacheBucket represents a bucket in the value cache with its own mutex
type valueCacheBucket struct {
    mutex sync.RWMutex
    values map[int64]*valueCacheEntry  // offset -> entry
}

// valueCacheEntry represents a cached value
type valueCacheEntry struct {
    key        []byte // The key associated with this cached value
    value      []byte // The cached value data
    accessTime uint64 // When this entry was last accessed
}

// externalValueEntry represents a value in the external value cache
type externalValueEntry struct {
	value       []byte
	txnSequence int64
	dirty       bool   // Whether this value needs to be written to disk
	next        *externalValueEntry
}

// externalKey represents an external key with its value and file tracking info
type externalKey struct {
	key            []byte              // The key bytes
	value          *externalValueEntry // Pointer to the current value entry
	sequenceNumber uint64              // Sequence number of the last valid file
	recordCount    int                 // Number of records in the current file
	openFile       *os.File            // Open file handle for writing (nil if not open)
}

// DB represents the database instance
type DB struct {
	databaseID     uint64 // Unique identifier for the database
	filePath       string
	mainFile       *os.File
	indexFile      *os.File
	readMutex      sync.RWMutex  // Mutex for reader coordination (Close, SetOption)
	writeMutex     sync.Mutex    // Mutex for writer serialization (Set, Begin, transactions)
	seqMutex       sync.Mutex    // Mutex for transaction state and sequence numbers
	mainIndexPages int   // Number of pages in main index
	mainFileSize   int64 // Track main file size to avoid frequent stat calls
	indexFileSize  int64 // Track index file size to avoid frequent stat calls
	prevFileSize   int64 // Track main file size before the current transaction started
	flushFileSize  int64 // Track main file size for flush operations
	cloningFileSize int64 // Track main file size when a cloning mark was created
	fileLocked     bool  // Track if the files are locked
	lockType       int   // Type of lock currently held
	readOnly       bool  // Track if the database is opened in read-only mode
	pageCache      [1024]cacheBucket // Page cache for all page types
	totalCachePages atomic.Int64     // Total number of pages in cache (including previous versions)
	valueCache     [256]valueCacheBucket // Value cache for frequently accessed values
	totalCacheValues atomic.Int64    // Total number of values in cache
	totalCacheMemory atomic.Int64    // Total memory used by value cache in bytes
	valueCacheAccessCounter atomic.Uint64 // Counter for value cache access times
	valueCacheThreshold int64        // Maximum memory in bytes for value cache before cleanup
	lastIndexedOffset int64 // Track the offset of the last indexed content in the main file
	writeMode      string // Current write mode
	nextWriteMode  string // Next write mode to apply
	commitMode     int    // CallerThread or WorkerThread
	useWAL         bool   // Whether to use WAL or not
	syncMode       int    // SyncOn or SyncOff
	walInfo        *WalInfo // WAL file information
	inTransaction  bool   // Track if inside of a transaction
	inExplicitTransaction bool // Track if an explicit transaction is open
	txnSequence    int64  // Current transaction sequence number
	flushSequence  int64  // Current flush up to this transaction sequence number
	maxReadSequence int64 // Maximum transaction sequence number that can be read
	pruningSequence int64 // Last transaction sequence number when cache pruning was performed
	cloningSequence int64 // Cloning mark sequence number
	fastRollback   bool   // Whether to use fast rollback (clone every transaction) or fast write (clone every 1000 transactions)
	txnChecksum    uint32 // Running CRC32 checksum for current transaction
	accessCounter  uint64 // Counter for page access times
	dirtyPageCount int    // Count of dirty pages in cache
	cacheSizeThreshold int // Maximum number of pages in cache before cleanup
	dirtyPageThreshold int // Maximum number of dirty pages before flush
	checkpointThreshold int64 // Maximum WAL file size in bytes before checkpoint
	workerChannel  chan string // Channel for background worker commands
	workerWaitGroup sync.WaitGroup // WaitGroup to coordinate with worker thread
	pendingCommands map[string]bool // Map to track pending worker commands
	originalLockType int // Original lock type before transaction
	lockAcquiredForTransaction bool // Whether lock was acquired for transaction
	headerPageForTransaction *Page // Pointer to the header page for transaction
	transactionCond *sync.Cond // Condition variable for transaction waiting
	lastFlushTime time.Time // Time of the last flush operation
	isClosed bool // Whether the database is closed
	externalKeys []*externalKey // List of external keys with their values and file info
}

// Transaction represents a database transaction
type Transaction struct {
	db *DB
	txnSequence int64
}

// Content represents a piece of content in the database
type Content struct {
	offset      int64 // File offset where this content is stored
	data        []byte
	key         []byte // Parsed key for ContentTypeData
	value       []byte // Parsed value for ContentTypeData
}

// Page is a unified struct containing fields for both TablePage and HybridPage
type Page struct {
	pageNumber   uint32
	pageType     byte
	data         []byte
	dirty        bool   // Whether this page contains unsaved changes
	isWAL        bool   // Whether this page is part of the WAL
	accessTime   uint64 // Last time this page was accessed
	txnSequence  int64  // Transaction sequence number
	next         *Page  // Pointer to the next entry with the same page number
	// Fields for TablePage
	Salt         uint8  // Salt for hash table pages
	// Fields for HybridPage
	NumSubPages  uint8                // Number of sub-pages on this page
	ContentSize  int                  // Total size of content on this page
	SubPages     []HybridSubPageInfo  // Information about sub-pages in this hybrid page
	// Fields for HeaderPage (only used when pageNumber == 0)
	freeHybridSpaceArray []FreeSpaceEntry // Array of hybrid pages with free space (allocated only for header page)
}

// TablePage is an alias for Page
type TablePage = Page

// HybridPage is an alias for Page
type HybridPage = Page

// HybridSubPage represents a reference to a specific sub-page within a hybrid page
type HybridSubPage struct {
	Page      *HybridPage  // Pointer to the parent hybrid page
	SubPageId uint8        // Index of the sub-page within the parent page
}

// HybridSubPageInfo stores metadata for a hybrid sub-page
type HybridSubPageInfo struct {
	Salt    uint8       // Salt for this sub-page
	Offset  uint16      // Offset in the page data where the sub-page starts
	Size    uint16      // Size of the sub-page data (excluding header)
}

// Options represents configuration options for the database
type Options map[string]interface{}

// DebugMode controls whether debug prints are enabled
var DebugMode bool

// init initializes package-level variables
func init() {
	// Check for debug mode environment variable
	debugEnv := os.Getenv("HASHTABLEDB_DEBUG")
	// Any non-empty value enables debug mode
	if debugEnv != "" {
		DebugMode = true
	}
}

// debugPrint prints a message if debug mode is enabled
func debugPrint(format string, args ...interface{}) {
	if DebugMode {
		fmt.Printf(format, args...)
	}
}

// Open opens or creates a database file with the given options
func Open(path string, options ...Options) (*DB, error) {

	mainFileExists := false
	if _, err := os.Stat(path); err == nil {
		mainFileExists = true
	}

	// Generate index file path by adding '-index' suffix
	indexPath := path + "-index"
	indexFileExists := false
	if _, err := os.Stat(indexPath); err == nil {
		indexFileExists = true
	}

	if !mainFileExists && indexFileExists {
		// Remove index file if main file doesn't exist
		os.Remove(indexPath)
		indexFileExists = false
	}
	if !indexFileExists {
		// Remove WAL file if index file doesn't exist
		os.Remove(path + "-wal")
	}

	// Default options
	lockType := LockExclusive // Default to use an exclusive lock
	readOnly := false
	writeMode := WorkerThread_WAL // Default to use WAL in a background thread
	mainIndexPages := DefaultMainIndexPages            // Default number of main index pages
	cacheSizeThreshold := calculateDefaultCacheSize()  // Calculate based on system memory
	dirtyPageThreshold := cacheSizeThreshold / 2       // Default to 50% of cache size
	checkpointThreshold := int64(1024 * 1024 * 100)    // Default to 100MB
	fastRollback := true                               // Default to slower transaction, faster rollback
	valueCacheThreshold := int64(DefaultValueCacheThreshold) // Default value cache memory threshold

	// Parse options
	var opts Options
	if len(options) > 0 {
		opts = options[0]
	}
	if opts != nil {
		/*
		if val, ok := opts["LockType"]; ok {
			if lt, ok := val.(int); ok {
				lockType = lt
			}
		}
		*/
		if val, ok := opts["ReadOnly"]; ok {
			if ro, ok := val.(bool); ok {
				readOnly = ro
			}
		}
		if val, ok := opts["HashTableSize"]; ok {
			if pages, ok := val.(int); ok && pages > 0 {
				mainIndexPages = pages
			} else {
				return nil, fmt.Errorf("invalid value for HashTableSize option")
			}
		}
		if val, ok := opts["WriteMode"]; ok {
			if jm, ok := val.(string); ok {
				if jm == CallerThread_WAL_Sync || jm == CallerThread_WAL_NoSync || jm == WorkerThread_WAL || jm == WorkerThread_NoWAL || jm == WorkerThread_NoWAL_NoSync {
					writeMode = jm
				} else {
					return nil, fmt.Errorf("invalid value for WriteMode option")
				}
			}
		}
		if val, ok := opts["CacheSizeThreshold"]; ok {
			if cst, ok := val.(int); ok && cst > 0 {
				cacheSizeThreshold = cst
			}
		}
		if val, ok := opts["DirtyPageThreshold"]; ok {
			if dpt, ok := val.(int); ok && dpt > 0 {
				dirtyPageThreshold = dpt
			}
		}
		// Value cache configuration
		if val, ok := opts["ValueCacheThreshold"]; ok {
			if vct, ok := val.(int64); ok && vct >= 0 {
				valueCacheThreshold = vct
			} else if vct, ok := val.(int); ok && vct >= 0 {
				valueCacheThreshold = int64(vct)
			}
		}
		if val, ok := opts["CheckpointThreshold"]; ok {
			if cpt, ok := val.(int64); ok && cpt > 0 {
				checkpointThreshold = cpt
			} else if cpt, ok := val.(int); ok && cpt > 0 {
				checkpointThreshold = int64(cpt)
			}
		}
		if val, ok := opts["FastRollback"]; ok {
			if fr, ok := val.(bool); ok {
				fastRollback = fr
			}
		}
	}

	// Open main file with appropriate flags
	var mainFile *os.File
	var err error
	if readOnly {
		mainFile, err = os.OpenFile(path, os.O_RDONLY, 0666)
		if err != nil {
			return nil, fmt.Errorf("failed to open main database file in read-only mode: %w", err)
		}
	} else {
		mainFile, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			return nil, fmt.Errorf("failed to open main database file: %w", err)
		}
	}

	// Open index file with appropriate flags
	var indexFile *os.File
	if readOnly {
		indexFile, err = os.OpenFile(indexPath, os.O_RDONLY, 0666)
		if err != nil {
			mainFile.Close()
			return nil, fmt.Errorf("failed to open index database file in read-only mode: %w", err)
		}
	} else {
		indexFile, err = os.OpenFile(indexPath, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			mainFile.Close()
			return nil, fmt.Errorf("failed to open index database file: %w", err)
		}
	}

	// Get initial file sizes
	mainFileInfo, err := mainFile.Stat()
	if err != nil {
		mainFile.Close()
		indexFile.Close()
		return nil, fmt.Errorf("failed to get main file size: %w", err)
	}

	indexFileInfo, err := indexFile.Stat()
	if err != nil {
		mainFile.Close()
		indexFile.Close()
		return nil, fmt.Errorf("failed to get index file size: %w", err)
	}

	db := &DB{
		databaseID:         0, // Will be set on read or initialize
		filePath:           path,
		mainFile:           mainFile,
		indexFile:          indexFile,
		mainIndexPages:     mainIndexPages,
		mainFileSize:       mainFileInfo.Size(),
		indexFileSize:      indexFileInfo.Size(),
		readOnly:           readOnly,
		lockType:           LockNone,
		dirtyPageCount:     0,
		dirtyPageThreshold: dirtyPageThreshold,
		cacheSizeThreshold: cacheSizeThreshold,
		valueCacheThreshold: valueCacheThreshold,
		checkpointThreshold: checkpointThreshold,
		fastRollback:       fastRollback,
		workerChannel:      make(chan string, 10), // Buffer size of 10 for commands
		pendingCommands:    make(map[string]bool), // Initialize the pending commands map
		lastFlushTime:      time.Now(), // Initialize to current time
	}

	// Initialize each bucket's map
	for i := range db.pageCache {
		db.pageCache[i].pages = make(map[uint32]*Page)
	}

	// Initialize the total cache pages counter
	db.totalCachePages.Store(0)

	// Initialize each value cache bucket's map
	for i := range db.valueCache {
		db.valueCache[i].values = make(map[int64]*valueCacheEntry)
	}

	// Initialize the value cache counters
	db.totalCacheValues.Store(0)
	db.totalCacheMemory.Store(0)
	db.valueCacheAccessCounter.Store(0)

	// Initialize the transaction condition variable
	db.transactionCond = sync.NewCond(&db.writeMutex)

	// Ensure indexFileSize is properly aligned to page boundaries for existing files
	if indexFileExists && indexFileInfo.Size() > 0 {
		// Round up to the nearest page boundary to ensure correct page allocation
		actualPages := (indexFileInfo.Size() + PageSize - 1) / PageSize
		db.indexFileSize = actualPages * PageSize
	}

	// Initialize internal write mode fields
	db.updateWriteMode(writeMode)
	db.nextWriteMode = writeMode

	// Apply file lock if requested
	if lockType != LockNone {
		if err := db.Lock(lockType); err != nil {
			mainFile.Close()
			indexFile.Close()
			return nil, fmt.Errorf("failed to lock database files: %w", err)
		}
	}

	// Check if we need to initialize the database
	needsInitialization := !mainFileExists && !readOnly

	// Check if we need to rebuild the index
	needsIndexInitialization := mainFileExists && (!indexFileExists || indexFileInfo.Size() == 0) && !readOnly

	if needsInitialization {
		// Initialize new database
		if err := db.initialize(); err != nil {
			db.Unlock()
			mainFile.Close()
			indexFile.Close()
			return nil, fmt.Errorf("failed to initialize database: %w", err)
		}
	} else if needsIndexInitialization {
		// Main file exists but index file is missing or empty
		// Read the main file header first to get the database ID
		if err := db.readMainFileHeader(); err != nil {
			db.Unlock()
			mainFile.Close()
			indexFile.Close()
			return nil, fmt.Errorf("failed to read main file header: %w", err)
		}
		// Initialize a new index file
		debugPrint("Index file missing or empty, initializing new index\n")
		if err := db.initializeIndexFile(); err != nil {
			db.Unlock()
			mainFile.Close()
			indexFile.Close()
			return nil, fmt.Errorf("failed to initialize index file: %w", err)
		}
		debugPrint("Index file rebuilt successfully\n")
	} else {
		// Read existing database headers
		if err := db.readHeader(); err != nil {
			db.Unlock()
			mainFile.Close()
			indexFile.Close()
			return nil, fmt.Errorf("failed to read database header: %w", err)
		}
	}

	// If the index file is not up-to-date, reindex the remaining content
	if db.lastIndexedOffset < db.mainFileSize {
		if err := db.recoverUnindexedContent(); err != nil {
			db.Unlock()
			mainFile.Close()
			indexFile.Close()
			return nil, fmt.Errorf("failed to reindex database: %w", err)
		}
	}

	// readExternalValues
	if err := db.readExternalValues(); err != nil {
		db.Unlock()
		mainFile.Close()
		indexFile.Close()
		return nil, fmt.Errorf("failed to read external values: %w", err)
	}

	// Ensure txnSequence starts at 1
	if db.txnSequence == 0 {
		db.txnSequence = 1
	}

	// Start the background worker if not in read-only mode and using worker thread mode
	if !db.readOnly && db.commitMode == WorkerThread {
		db.startBackgroundWorker()
	}

	// Set a finalizer to close the database if it is not closed
	runtime.SetFinalizer(db, func(d *DB) {
		_ = d.Close()
	})

	return db, nil
}

// SetOption sets a database option after the database is open
func (db *DB) SetOption(name string, value interface{}) error {
	// Acquire both mutexes since this function modifies config that both readers and writers read
	db.writeMutex.Lock()
	db.readMutex.Lock()
	defer func() {
		db.readMutex.Unlock()
		db.writeMutex.Unlock()
	}()

	switch name {
	case "AddExternalKey":
		if key, ok := value.([]byte); ok {
			// check if the key already exists
			for _, extKey := range db.externalKeys {
				if bytes.Equal(extKey.key, key) {
					return nil
				}
			}
			// add the key to the list
			valueEntry := &externalValueEntry{
				value: []byte{},
				txnSequence: 0,
				dirty: false,
				next: nil,
			}
			db.externalKeys = append(db.externalKeys, &externalKey{
				key: key,
				value: valueEntry,
				sequenceNumber: 0,
				recordCount: 0,
			})
			return nil
		}
		return fmt.Errorf("AddExternalKey value must be a byte array")
	/*
	case "WriteMode":
		if jm, ok := value.(string); ok {
			if jm == CallerThread_WAL_Sync || jm == CallerThread_WAL_NoSync || jm == WorkerThread_WAL || jm == WorkerThread_NoWAL || jm == WorkerThread_NoWAL_NoSync {
				db.nextWriteMode = jm
				return nil
			}
			return fmt.Errorf("invalid value for WriteMode option")
		}
		return fmt.Errorf("WriteMode option value must be a string")
	*/
	case "CacheSizeThreshold":
		if cst, ok := value.(int); ok {
			if cst > 0 {
				db.cacheSizeThreshold = cst
				return nil
			}
			return fmt.Errorf("CacheSizeThreshold must be greater than 0")
		}
		return fmt.Errorf("CacheSizeThreshold value must be an integer")
	case "DirtyPageThreshold":
		if dpt, ok := value.(int); ok {
			if dpt > 0 {
				db.dirtyPageThreshold = dpt
				return nil
			}
			return fmt.Errorf("DirtyPageThreshold must be greater than 0")
		}
		return fmt.Errorf("DirtyPageThreshold value must be an integer")
	case "CheckpointThreshold":
		if cpt, ok := value.(int64); ok {
			if cpt > 0 {
				db.checkpointThreshold = cpt
				return nil
			}
			return fmt.Errorf("CheckpointThreshold must be greater than 0")
		}
		// Try to convert from int if int64 conversion failed
		if cpt, ok := value.(int); ok {
			if cpt > 0 {
				db.checkpointThreshold = int64(cpt)
				return nil
			}
			return fmt.Errorf("CheckpointThreshold must be greater than 0")
		}
		return fmt.Errorf("CheckpointThreshold value must be an integer")
	/*
	case "FastRollback":
		if fr, ok := value.(bool); ok {
			db.fastRollback = fr
			return nil
		}
		return fmt.Errorf("FastRollback value must be a boolean")
		*/
	case "ValueCacheThreshold":
		if vct, ok := value.(int64); ok {
			if vct >= 0 {
				db.valueCacheThreshold = vct
				return nil
			}
			return fmt.Errorf("ValueCacheThreshold must be greater than 0")
		}
		// Try to convert from int if int64 conversion failed
		if vct, ok := value.(int); ok {
			if vct >= 0 {
				db.valueCacheThreshold = int64(vct)
				return nil
			}
			return fmt.Errorf("ValueCacheThreshold must be greater than 0")
		}
		return fmt.Errorf("ValueCacheThreshold value must be an integer")
	default:
		return fmt.Errorf("unknown or immutable option: %s", name)
	}
}

// updateWriteMode updates the internal write mode fields based on the writeMode string
func (db *DB) updateWriteMode(writeMode string) {
	// Update the write mode
	db.writeMode = writeMode

	// Update the internal fields based on the write mode
	switch db.writeMode {
	case CallerThread_WAL_Sync:
		db.commitMode = CallerThread
		db.useWAL = true
		db.syncMode = SyncOn

	case CallerThread_WAL_NoSync:
		db.commitMode = CallerThread
		db.useWAL = true
		db.syncMode = SyncOff

	case WorkerThread_WAL:
		db.commitMode = WorkerThread
		db.useWAL = true
		db.syncMode = SyncOff

	case WorkerThread_NoWAL:
		db.commitMode = WorkerThread
		db.useWAL = false
		db.syncMode = SyncOn

	case WorkerThread_NoWAL_NoSync:
		db.commitMode = WorkerThread
		db.useWAL = false
		db.syncMode = SyncOff
	}
}

// Lock acquires a lock on the database file based on the specified lock type
func (db *DB) Lock(lockType int) error {
	var lockFlag int

	if db.fileLocked && db.lockType == lockType {
		return nil // Already locked with the same lock type
	}

	// If already locked with a different lock type, unlock first
	if db.fileLocked {
		if err := db.Unlock(); err != nil {
			return err
		}
	}

	switch lockType {
	case LockShared:
		lockFlag = syscall.LOCK_SH | syscall.LOCK_NB
		debugPrint("Acquiring shared lock on database file\n")
	case LockExclusive:
		lockFlag = syscall.LOCK_EX | syscall.LOCK_NB
		debugPrint("Acquiring exclusive lock on database file\n")
	default:
		return fmt.Errorf("invalid lock type: %d", lockType)
	}

	err := syscall.Flock(int(db.mainFile.Fd()), lockFlag)
	if err != nil {
		if lockType == LockShared {
			return fmt.Errorf("cannot acquire shared lock (another process may have an exclusive lock): %w", err)
		}
		return fmt.Errorf("cannot acquire exclusive lock (file may be in use): %w", err)
	}

	db.fileLocked = true
	db.lockType = lockType
	return nil
}

// Unlock releases the lock on the database file
func (db *DB) Unlock() error {
	if !db.fileLocked {
		return nil // Not locked
	}

	err := syscall.Flock(int(db.mainFile.Fd()), syscall.LOCK_UN)
	if err != nil {
		return fmt.Errorf("cannot release lock: %w", err)
	}

	db.fileLocked = false
	db.lockType = LockNone
	debugPrint("Database file unlocked\n")
	return nil
}

// acquireWriteLock temporarily acquires an exclusive lock for writing
func (db *DB) acquireWriteLock() error {
	// If we already have an exclusive lock, nothing to do
	if db.fileLocked && db.lockType == LockExclusive {
		return nil
	}

	// Acquire an exclusive lock
	return db.Lock(LockExclusive)
}

// releaseWriteLock releases a temporary write lock
// If the DB was originally opened with a different lock type, restore it
func (db *DB) releaseWriteLock(originalLockType int) error {
	// If the original lock type was none, just unlock
	if originalLockType == LockNone {
		return db.Unlock()
	}

	// Otherwise restore the original lock type
	return db.Lock(originalLockType)
}

// Close closes the database files
func (db *DB) Close() error {
	var mainErr, indexErr, flushErr error

	// STEP 1: Block new writers first
	db.writeMutex.Lock()
	defer db.writeMutex.Unlock()

	// Check if already closed
	if db.mainFile == nil && db.indexFile == nil {
		return nil // Already closed
	}
	if db.isClosed {
		return nil // Already closed
	}

	// Mark as closing
	// This will also avoid WAL checkpointing when doing the last WAL commit
	db.isClosed = true

	if !db.readOnly {
		// If there's an open transaction, rollback before closing
		if db.inTransaction {
			db.rollbackTransaction()
		}

		// Wake up all threads waiting for transactions to complete
		if db.transactionCond != nil {
			db.inExplicitTransaction = false
			db.transactionCond.Broadcast()
		}

		// STEP 2: Shutdown worker thread while holding writeMutex (blocks new writes)
		if db.commitMode == WorkerThread {
			// Signal the worker thread to flush the index to disk, even if
			// a flush is already running (to flush the remaining pages)
			db.workerChannel <- "flush"

			// Signal the worker thread to exit
			db.workerChannel <- "exit"

			// Wait for the worker thread to finish
			db.workerWaitGroup.Wait()

			// Close the channel
			close(db.workerChannel)
		}
	}

	// STEP 3: Now acquire readMutex (blocks readers, worker is done)
	db.readMutex.Lock()
	defer db.readMutex.Unlock()

	// If not using worker thread mode, flush on main thread
	if !db.readOnly && db.commitMode == CallerThread {
		flushErr = db.flushIndexToDisk()
	}

	// Close main file if open
	if db.mainFile != nil {
		// Release lock if acquired
		if db.fileLocked {
			if err := db.Unlock(); err != nil {
				mainErr = fmt.Errorf("failed to unlock database files: %w", err)
			}
		}
		// Close the file
		mainErr = db.mainFile.Close()
		db.mainFile = nil
	}

	// Close index file if open
	if db.indexFile != nil {
		indexErr = db.indexFile.Close()
		db.indexFile = nil
	}

	// Close all external value files
	db.closeExternalFiles()

	// Return first error encountered
	if flushErr != nil {
		return flushErr
	}
	if mainErr != nil {
		return mainErr
	}
	return indexErr
}

// Delete removes a key from the database
func (db *DB) Delete(key []byte) error {
	// Call set() with nil value to mark as deleted
	return db.set(key, nil, false)
}

// Set sets a key-value pair in the database
func (db *DB) Set(key, value []byte) error {
	return db.set(key, value, false)
}

func (db *DB) set(key, value []byte, calledByTransaction bool) error {
	// Check if database is closed
	if db.isClosed {
		return fmt.Errorf("the database is closed")
	}
	// Check if a transaction is open but this method wasn't called by the transaction object
	if db.inExplicitTransaction && !calledByTransaction {
		return fmt.Errorf("a transaction is open, use the transaction object instead")
	}
	// Check if file is opened in read-only mode
	if db.readOnly {
		return fmt.Errorf("cannot write: database opened in read-only mode")
	}

	// Validate key length
	keyLen := len(key)
	if keyLen == 0 {
		return fmt.Errorf("key cannot be empty")
	}
	if keyLen > MaxKeyLength {
		return fmt.Errorf("key length exceeds maximum allowed size of %d bytes", MaxKeyLength)
	}

	// Lock the database for writing
	db.writeMutex.Lock()

	// Start a transaction if not already in one
	if !db.inExplicitTransaction {
		err := db.beginTransaction()
		if err != nil {
			return fmt.Errorf("failed to begin transaction: %w", err)
		}
	}

	// Set the key-value pair
	err := db.set2(key, value)

	// Commit or rollback the transaction if not in an explicit transaction
	if !db.inExplicitTransaction {
		if err == nil {
			db.commitTransaction()
		} else {
			db.rollbackTransaction()
		}
	}

	// Unlock the database
	db.writeMutex.Unlock()

	// Acquire a read lock
	db.readMutex.RLock()
	defer db.readMutex.RUnlock()

	// Check the page and value caches
	db.checkCache(true)

	// Return the error
	return err
}

// Internal function to set a key-value pair in the database
func (db *DB) set2(key, value []byte) error {

	// Check if the key is a external key
	for _, extKey := range db.externalKeys {
		if equal(extKey.key, key) {
			// Update the value in the external value cache
			entry := extKey.value
			db.seqMutex.Lock()
			if entry.txnSequence == db.txnSequence {
				// Replace the value in the cache
				entry.value = value
				entry.dirty = true
			} else {
				// Add a new value to the cache
				newEntry := &externalValueEntry{
					value: value,
					txnSequence: db.txnSequence,
					dirty: true,
					next: entry,
				}
				extKey.value = newEntry
			}
			db.seqMutex.Unlock()
			return nil
		}
	}

	// Hash the key with initial salt
	hash := hashKey(key, InitialSalt)

	// Calculate total entries in main index
	totalMainEntries := uint64(db.mainIndexPages * TableEntries)

	// Determine which page of the main index to use
	mainIndexSlot := int(hash % totalMainEntries)
	pageNumber := uint32(mainIndexSlot/TableEntries + 1) // Main index starts at page 1
	slotInPage := mainIndexSlot % TableEntries

	// Get the main index page
	mainIndexPage, err := db.getTablePage(pageNumber)
	if err != nil {
		return fmt.Errorf("failed to get main index page %d: %w", pageNumber, err)
	}

	return db.setOnTablePage(mainIndexPage, key, value, 0, slotInPage)
}

// setKvOnIndex sets an existing key-value pair on the index (reindexing)
func (db *DB) setKvOnIndex(key, value []byte, dataOffset int64) error {
	// Hash the key with initial salt
	hash := hashKey(key, InitialSalt)

	// Calculate total entries in main index
	totalMainEntries := uint64(db.mainIndexPages * TableEntries)

	// Determine which page of the main index to use
	mainIndexSlot := int(hash % totalMainEntries)
	pageNumber := uint32(mainIndexSlot/TableEntries + 1) // Main index starts at page 1
	slotInPage := mainIndexSlot % TableEntries

	// Get the main index page
	mainIndexPage, err := db.getTablePage(pageNumber)
	if err != nil {
		return fmt.Errorf("failed to get main index page %d: %w", pageNumber, err)
	}

	return db.setOnTablePage(mainIndexPage, key, value, dataOffset, slotInPage)
}

// setOnTablePage sets a key-value pair in a table page
func (db *DB) setOnTablePage(tablePage *TablePage, key, value []byte, dataOffset int64, forcedSlot ...int) error {
	// Check if we're deleting (value is nil)
	isDelete := len(value) == 0

	// Calculate slot for the key
	var slot int
	if tablePage.Salt == InitialSalt && len(forcedSlot) > 0 {
		// Use the forced slot for main index (salt 0)
		slot = forcedSlot[0]
	} else {
		// Calculate slot for the key
		slot = db.getTableSlot(key, tablePage.Salt)
	}

	// Check if slot has an entry
	pageNumber, subPageId := db.getTableEntry(tablePage, slot)

	// If there's no entry for this slot, we need to create a hybrid page to store the data offset
	if pageNumber == 0 {
		debugPrint("setOnTablePage page %d slot %d: empty\n", tablePage.pageNumber, slot)

		// If we're deleting, nothing to do
		if isDelete {
			return nil
		}

		// If dataOffset is 0, we're setting a new key-value pair
		if dataOffset == 0 {
			// Append the data to the main file
			var err error
			dataOffset, err = db.appendData(key, value)
			if err != nil {
				return fmt.Errorf("failed to append data: %w", err)
			}
		}

		// Create a new hybrid sub-page to store this entry
		childSubPage, err := db.addEntryToNewHybridSubPage(tablePage.Salt, key, dataOffset)
		if err != nil {
			return fmt.Errorf("failed to add entry to new hybrid sub-page: %w", err)
		}

		// Get writable version of the table page
		tablePage, err = db.getWritablePage(tablePage)
		if err != nil {
			return fmt.Errorf("failed to get writable page: %w", err)
		}

		// Store reference to the child page on the parent page
		debugPrint("updating page %d slot %d: page %d subPageId %d\n", tablePage.pageNumber, slot, childSubPage.Page.pageNumber, childSubPage.SubPageId)
		return db.setTableEntry(tablePage, slot, childSubPage.Page.pageNumber, childSubPage.SubPageId)
	}

	debugPrint("setOnTablePage page %d slot %d: pageNumber %d subPageId %d\n", tablePage.pageNumber, slot, pageNumber, subPageId)

	// Slot is used - it's a page pointer, load the page and continue
	page, err := db.getPage(pageNumber)
	if err != nil {
		return fmt.Errorf("failed to load page %d: %w", pageNumber, err)
	}

	if page.pageType == ContentTypeTable {
		// It's another table page, continue with table lookup
		nextTablePage := (*TablePage)(page)
		return db.setOnTablePage(nextTablePage, key, value, dataOffset)
	} else if page.pageType == ContentTypeHybrid {
		// It's a hybrid page, use the subPageId
		nextSubPage := &HybridSubPage{
			Page:       page,
			SubPageId:  subPageId,
		}
		err = db.setOnHybridSubPage(nextSubPage, key, value, dataOffset)
		if err != nil {
			return fmt.Errorf("failed to set on hybrid sub-page: %w", err)
		}
		// If the sub-page was moved to a new page or its sub-page index changed, update the pointer
		if nextSubPage.Page.pageNumber != page.pageNumber || nextSubPage.SubPageId != subPageId {
			// Get writable version of the table page
			tablePage, err = db.getWritablePage(tablePage)
			if err != nil {
				return fmt.Errorf("failed to get writable page: %w", err)
			}
			// Store reference to the child page on the parent page
			debugPrint("updating page %d slot %d: moved to page %d subPageId %d\n", tablePage.pageNumber, slot, nextSubPage.Page.pageNumber, nextSubPage.SubPageId)
			return db.setTableEntry(tablePage, slot, nextSubPage.Page.pageNumber, nextSubPage.SubPageId)
		}
		return nil
	} else {
		return fmt.Errorf("invalid page type: %c", page.pageType)
	}
}

// setOnHybridSubPage attempts to set a key-value pair in an existing hybrid sub-page
// If dataOffset is 0, we're setting a new key-value pair
// Otherwise, it means we're reindexing already stored key-value pair
func (db *DB) setOnHybridSubPage(subPage *HybridSubPage, key, value []byte, dataOffset int64) error {
	hybridPage := subPage.Page
	subPageId := subPage.SubPageId

	// Get the sub-page info to get the salt
	if int(subPageId) >= len(hybridPage.SubPages) || hybridPage.SubPages[subPageId].Offset == 0 {
		return fmt.Errorf("sub-page with index %d not found", subPageId)
	}
	subPageInfo := &hybridPage.SubPages[subPageId]

	// Check if we're deleting
	isDelete := len(value) == 0

	// Calculate the target slot using the sub-page's salt
	slot := db.getTableSlot(key, subPageInfo.Salt)

	// Search in the specific sub-page
	entryOffset, entrySize, isSubPage, value64, err := db.findEntryInHybridSubPage(hybridPage, subPageId, slot)
	if err != nil {
		return fmt.Errorf("failed to search in hybrid sub-page: %w", err)
	}

	// If entry not found in the sub-page
	if value64 == 0 {
		debugPrint("setOnHybridSubPage page %d sub-page %d slot %d: empty\n", hybridPage.pageNumber, subPageId, slot)

		// If we're deleting and didn't find the key, nothing to do
		if isDelete {
			return nil
		}

		// If we're setting a new key-value pair
		if dataOffset == 0 {
			// Append new data to the main file
			dataOffset, err = db.appendData(key, value)
			if err != nil {
				return fmt.Errorf("failed to append data: %w", err)
			}
		}

		// Try to add the entry to this sub-page
		// If the hybrid sub-page is full, it will be converted to a table page
		return db.addEntryToHybridSubPage(subPage, slot, dataOffset)
	}

	// Entry found
	if isSubPage {
		// It's a sub-page pointer, follow it
		nextSubPageId := uint8(value64 & 0xFF)
		nextPageNumber := uint32((value64 >> 8) & 0xFFFFFFFF)

		debugPrint("setOnHybridSubPage page %d sub-page %d slot %d: pageNumber %d subPageId %d\n", hybridPage.pageNumber, subPageId, slot, nextPageNumber, nextSubPageId)

		// Load the next page
		page, err := db.getPage(nextPageNumber)
		if err != nil {
			return fmt.Errorf("failed to load page %d: %w", nextPageNumber, err)
		}

		if page.pageType == ContentTypeTable {
			// It's a table page
			nextTablePage := (*TablePage)(page)
			return db.setOnTablePage(nextTablePage, key, value, dataOffset)
		} else if page.pageType == ContentTypeHybrid {
			// It's a hybrid page
			nextSubPage := &HybridSubPage{
				Page:       page,
				SubPageId:  nextSubPageId,
			}

			// Store the current sub-page offset
			previousSubPageOffset := subPageInfo.Offset

			err = db.setOnHybridSubPage(nextSubPage, key, value, dataOffset)
			if err != nil {
				return fmt.Errorf("failed to set on hybrid sub-page: %w", err)
			}

			// Update the subPage pointer, because the above function
			// could have cloned the same hybrid page used by this subPage
			if nextSubPage.Page.pageNumber == subPage.Page.pageNumber {
				// If it's still the same page number, use the potentially updated reference
				subPage.Page = nextSubPage.Page
			}
			/*
			subPage.Page, err = db.getHybridPage(subPage.Page.pageNumber)
			if err != nil {
				return fmt.Errorf("failed to get hybrid page: %w", err)
			}
			*/

			// If the sub-page was moved to a new page or its sub-page index changed, update the pointer
			if nextSubPage.Page.pageNumber != nextPageNumber || nextSubPage.SubPageId != nextSubPageId {
				// If the sub-page was moved to a new position, update the entry offset
				if subPageInfo.Offset != previousSubPageOffset {
					entryOffset += int(subPageInfo.Offset) - int(previousSubPageOffset)
				}
				// Store reference to the child page on the parent page
				debugPrint("Updating page %d sub-page %d slot %d: Storing pageNumber %d subPageId %d\n", hybridPage.pageNumber, subPageId, slot, nextSubPage.Page.pageNumber, nextSubPage.SubPageId)
				return db.updateSubPagePointerInHybridSubPage(subPage, entryOffset, entrySize, nextSubPage.Page.pageNumber, nextSubPage.SubPageId)
			}
			return nil
		} else {
			return fmt.Errorf("invalid page type: %c", page.pageType)
		}

	} else {
		// It's a data offset
		existingDataOffset := int64(value64)

		debugPrint("setOnHybridSubPage page %d sub-page %d slot %d: dataOffset %d\n", hybridPage.pageNumber, subPageId, slot, existingDataOffset)

		// Read the content at the offset
		content, err := db.readContent(existingDataOffset)
		if err != nil {
			return fmt.Errorf("failed to read content: %w", err)
		}

		// Compare the key
		if equal(content.key, key) {
			// It is the same key

			// If we're deleting
			if isDelete {
				// If there is an existing value
				if dataOffset == 0 && content != nil && len(content.value) > 0 {
					// Log the deletion to the main file
					dataOffset, err = db.appendData(key, nil)
					if err != nil {
						return fmt.Errorf("failed to append deletion: %w", err)
					}
				}
				// Remove this entry from the sub-page
				debugPrint("removing entry from page %d sub-page %d slot %d\n", hybridPage.pageNumber, subPageId, slot)
				return db.removeEntryFromHybridSubPage(subPage, entryOffset, entrySize)
			}

			// If we're setting a new key-value pair
			if dataOffset == 0 {
				// Check if value is the same
				if equal(content.value, value) {
					// Value is the same, nothing to do
					return nil
				}

				// Value is different, append new data
				dataOffset, err = db.appendData(key, value)
				if err != nil {
					return fmt.Errorf("failed to append data: %w", err)
				}
			}

			// Update the entry's data offset in the sub-page
			debugPrint("updating page %d sub-page %d slot %d: dataOffset %d\n", hybridPage.pageNumber, subPageId, slot, dataOffset)
			return db.updateDataOffsetInHybridSubPage(subPage, entryOffset, entrySize, dataOffset)

		} else {
			// Different key

			// If we're deleting and didn't find the key, nothing to do
			if isDelete {
				// Key not found, nothing to do
				return nil
			}

			debugPrint("collision: different key at offset %d, adding both entries to new sub-page\n", existingDataOffset)

			// If we're setting a new key-value pair
			if dataOffset == 0 {
				// Append new data to the main file
				dataOffset, err = db.appendData(key, value)
				if err != nil {
					return fmt.Errorf("failed to append data: %w", err)
				}
			}

			// Handle hash collision by inserting both entries into a new sub-page
			// It will find a salt that avoids collisions
			entries := []HybridEntry{
				{Key: content.key, DataOffset: existingDataOffset},
				{Key: key, DataOffset: dataOffset},
			}
			newSubPage, err := db.addEntriesToNewHybridSubPage(subPageInfo.Salt, entries)
			if err != nil {
				return fmt.Errorf("failed to create new sub-page for collision: %w", err)
			}

			// If the new sub-page was created on the same page, update our reference to use the latest version
			if newSubPage.Page.pageNumber == subPage.Page.pageNumber {
				subPage.Page = newSubPage.Page
			}
			/*
			subPage.Page, err = db.getHybridPage(subPage.Page.pageNumber)
			if err != nil {
				return fmt.Errorf("failed to get hybrid page: %w", err)
			}
			*/

			// Convert the data offset entry to a sub-page pointer entry
			// This replaces the 8-byte data offset with a 5-byte sub-page pointer
			debugPrint("converting entry in hybrid page %d sub-page %d slot %d: now pointer to page %d sub-page %d\n", subPage.Page.pageNumber, subPage.SubPageId, slot, newSubPage.Page.pageNumber, newSubPage.SubPageId)
			err = db.convertEntryInHybridSubPage(subPage, entryOffset, entrySize, newSubPage.Page.pageNumber, newSubPage.SubPageId)
			if err != nil {
				return fmt.Errorf("failed to convert entry to sub-page pointer: %w", err)
			}
		}
	}

	return nil
}

// Get retrieves a value for the given key
func (db *DB) Get(key []byte) ([]byte, error) {
	return db.get(key, false)
}

// get is the internal method that retrieves a value for the given key with explicit transaction context
func (db *DB) get(key []byte, calledByTransaction bool) ([]byte, error) {
	// Check if database is closed
	if db.isClosed {
		return nil, fmt.Errorf("the database is closed")
	}

	// Lock for the entire read operation to coordinate with Close()
	db.readMutex.RLock()
	defer func() {
		db.checkCache(false)
		db.readMutex.RUnlock()
	}()

	// Validate key length
	keyLen := len(key)
	if keyLen == 0 {
		return nil, fmt.Errorf("key cannot be empty")
	}
	if keyLen > MaxKeyLength {
		return nil, fmt.Errorf("key length exceeds maximum allowed size of %d bytes", MaxKeyLength)
	}

	// Determine the maximum transaction sequence number that can be read
	var maxReadSequence int64
	db.seqMutex.Lock()
	if calledByTransaction || !db.inTransaction {
		maxReadSequence = db.txnSequence
	} else {
		// When FastRollback=false, db.Get() should see transaction changes
		// When FastRollback=true, db.Get() should not see transaction changes
		if db.fastRollback {
			maxReadSequence = db.txnSequence - 1
		} else {
			maxReadSequence = db.txnSequence
		}
	}
	db.seqMutex.Unlock()

	// Check if the key is a external key
	for _, extKey := range db.externalKeys {
		if equal(extKey.key, key) {
			// Return the value from the external value cache
			entry := extKey.value
			// Retrieve a value in which the txnSequence is less than or equal to maxReadSequence
			for entry != nil {
				if entry.txnSequence <= maxReadSequence {
					return entry.value, nil
				}
				entry = entry.next
			}
			return nil, fmt.Errorf("key not found")
		}
	}

	// Hash the key with initial salt
	hash := hashKey(key, InitialSalt)

	// Calculate total entries in main index
	totalMainEntries := uint64(db.mainIndexPages * TableEntries)

	// Determine which page of the main index to use
	mainIndexSlot := int(hash % totalMainEntries)
	pageNumber := uint32(mainIndexSlot/TableEntries + 1) // Main index starts at page 1
	slotInPage := mainIndexSlot % TableEntries

	// Load the main index page directly
	mainIndexPage, err := db.getTablePage(pageNumber, maxReadSequence)
	if err != nil {
		return nil, fmt.Errorf("failed to load main index page %d: %w", pageNumber, err)
	}

	// Get from the main index page using the forced slot
	return db.getFromTablePage(key, mainIndexPage, maxReadSequence, slotInPage)
}

// getFromPage loads a page and dispatches to the appropriate function based on page type
func (db *DB) getFromPage(key []byte, pageNumber uint32, subPageId uint8, maxReadSequence int64) ([]byte, error) {
  // If there's no entry for this page, the key doesn't exist
  if pageNumber == 0 {
    return nil, fmt.Errorf("key not found")
  }

	// Load the page from cache/disk
	page, err := db.getPage(pageNumber, maxReadSequence)
	if err != nil {
		return nil, fmt.Errorf("failed to load page %d: %w", pageNumber, err)
	}

	// Check the page type and handle accordingly
	if page.pageType == ContentTypeTable {
		// It's a table page, call getFromTable (don't use subPageId)
		tablePage := (*TablePage)(page)
		return db.getFromTablePage(key, tablePage, maxReadSequence)
	} else if page.pageType == ContentTypeHybrid {
		// It's a hybrid page, use the subPageId to access the specific sub-page
		hybridPage := (*HybridPage)(page)
		return db.getFromHybridSubPage(key, hybridPage, subPageId, maxReadSequence)
	} else {
		return nil, fmt.Errorf("invalid page type: %c", page.pageType)
	}
}

// getFromTablePage retrieves a value using the specified key and table page
func (db *DB) getFromTablePage(key []byte, tablePage *TablePage, maxReadSequence int64, forcedSlot ...int) ([]byte, error) {
	// Calculate the target slot using the table page's salt
	var slot int
	if tablePage.Salt == InitialSalt && len(forcedSlot) > 0 {
		// Use the forced slot for main index (salt 0)
		slot = forcedSlot[0]
	} else {
		// Calculate slot for the key
		slot = db.getTableSlot(key, tablePage.Salt)
	}

	// Check if slot has an entry
	pageNumber, subPageId := db.getTableEntry(tablePage, slot)
	if pageNumber == 0 {
		return nil, fmt.Errorf("key not found")
	}

	// Use getFromPage to handle page loading and type dispatching
	return db.getFromPage(key, pageNumber, subPageId, maxReadSequence)
}

// getFromHybridSubPage retrieves a value from a hybrid sub-page
func (db *DB) getFromHybridSubPage(key []byte, hybridPage *HybridPage, subPageId uint8, maxReadSequence int64) ([]byte, error) {
	// Get the sub-page info to get the salt
	if int(subPageId) >= len(hybridPage.SubPages) || hybridPage.SubPages[subPageId].Offset == 0 {
		return nil, fmt.Errorf("sub-page with index %d not found", subPageId)
	}
	subPageInfo := &hybridPage.SubPages[subPageId]

	// Calculate the target slot using the sub-page's salt
	slot := db.getTableSlot(key, subPageInfo.Salt)

	// Search in the specific sub-page
	_, _, isSubPage, value, err := db.findEntryInHybridSubPage(hybridPage, subPageId, slot)
	if err != nil {
		return nil, fmt.Errorf("failed to search in hybrid sub-page: %w", err)
	}

	if value == 0 {
		return nil, fmt.Errorf("key not found")
	}

	if isSubPage {
		// It's a sub-page pointer: extract page number and sub-page ID
		nextSubPageId := uint8(value & 0xFF)
		nextPageNumber := uint32((value >> 8) & 0xFFFFFFFF)

		// Use getFromPage to handle page loading and type dispatching
		return db.getFromPage(key, nextPageNumber, nextSubPageId, maxReadSequence)
	} else {
		// It's a data offset
		dataOffset := int64(value)

		// Read the content from cache or disk
		return db.readContentValue(dataOffset, key)
	}
}

// Helper functions

// initialize creates a new database file structure
func (db *DB) initialize() error {

	// If not already exclusively locked
	if db.lockType != LockExclusive {
		// Remember the original lock type
		originalLockType := db.lockType
		// Acquire a write lock
		if err := db.acquireWriteLock(); err != nil {
			return fmt.Errorf("failed to acquire write lock for initialization: %w", err)
		}
		// Release the lock on exit
		defer db.releaseWriteLock(originalLockType)
	}

	debugPrint("Initializing database\n")

	// Generate a random database ID
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	db.databaseID = r.Uint64()
	debugPrint("Generated new database ID: %d\n", db.databaseID)

	// Save the original journal mode and temporarily disable it during initialization
	originalWriteMode := db.writeMode
	db.updateWriteMode(WorkerThread_NoWAL_NoSync)

	// Initialize main file
	if err := db.initializeMainFile(); err != nil {
		return fmt.Errorf("failed to initialize main file: %w", err)
	}

	// Initialize index file
	if err := db.initializeIndexFile(); err != nil {
		return fmt.Errorf("failed to initialize index file: %w", err)
	}

	// Restore the original journal mode
	db.updateWriteMode(originalWriteMode)

	debugPrint("Database initialized\n")

	return nil
}

// initializeMainFile initializes the main data file
func (db *DB) initializeMainFile() error {
	// Write file header in root page (page 1)
	rootPage := make([]byte, PageSize)

	// Write the 6-byte magic string
	copy(rootPage[0:6], MainFileMagicString)

	// Write the 2-byte version
	copy(rootPage[6:8], VersionString)

	// Write the 8-byte database ID
	binary.LittleEndian.PutUint64(rootPage[8:16], db.databaseID)

	// The rest of the root page is reserved for future use

	debugPrint("Writing main file root page to disk\n")
	// Write the root page to the file
	if _, err := db.mainFile.Write(rootPage); err != nil {
		return err
	}

	// Update file size to include the root page
	db.mainFileSize = PageSize

	return nil
}

// initializeIndexFile initializes the index file
func (db *DB) initializeIndexFile() error {

	// Write the index file header
	debugPrint("Writing index file header\n")
	if err := db.writeIndexHeader(true); err != nil {
		return fmt.Errorf("failed to write index file header: %w", err)
	}

	// Create main index pages starting at page 1
	for pageNumber := 1; pageNumber <= db.mainIndexPages; pageNumber++ {
		// Create table page
		tablePage, err := db.allocateTablePage()
		if err != nil {
			return fmt.Errorf("failed to create main index page %d: %w", pageNumber, err)
		}

		if tablePage.pageNumber != uint32(pageNumber) {
			return fmt.Errorf("unexpected page number: %d, expected: %d", tablePage.pageNumber, pageNumber)
		}

		// Set salt to InitialSalt for main index pages
		tablePage.Salt = InitialSalt

		// Mark as dirty
		db.markPageDirty(tablePage)
	}

	// If using WAL mode, delete existing WAL file and open a new one
	if db.useWAL {
		// Delete existing WAL file
		if err := db.deleteWAL(); err != nil {
			return fmt.Errorf("failed to delete WAL file: %w", err)
		}
		// Open new WAL file
		if err := db.openWAL(); err != nil {
			return fmt.Errorf("failed to open WAL file: %w", err)
		}
	}

	// Flush the index to disk
	//if err := db.flushIndexToDisk(); err != nil {
	//	return fmt.Errorf("failed to flush index to disk: %w", err)
	//}

	return nil
}

// readHeader reads the database headers and preloads the first level of the table tree
func (db *DB) readHeader() error {
	// Read main file header
	if err := db.readMainFileHeader(); err != nil {
		return fmt.Errorf("failed to read main file header: %w", err)
	}

	// Read the index file header directly from the index file
	if err := db.readIndexFileHeader(false); err != nil {
		return fmt.Errorf("failed to read index file header: %w", err)
	}

	// Check for existing WAL file if in WAL mode
	if db.useWAL {
		// Open existing WAL file if it exists
		if err := db.openWAL(); err != nil {
			return fmt.Errorf("failed to open WAL file: %w", err)
		}
	}

	// Read the index file header from WAL/cache
	// This will update lastIndexedOffset and freePageNum with the latest values
	if err := db.readIndexFileHeader(true); err != nil {
		return fmt.Errorf("failed to read index file header from WAL: %w", err)
	}

	// Preload the main hash table
	if err := db.preloadMainHashTable(); err != nil {
		return fmt.Errorf("failed to preload main hash table: %w", err)
	}

	return nil
}

// readMainFileHeader reads the main file header
func (db *DB) readMainFileHeader() error {
	// Read the header (16 bytes) in root page (page 1)
	header := make([]byte, 16)
	if _, err := db.mainFile.ReadAt(header, 0); err != nil {
		return err
	}

	// Extract magic string (6 bytes)
	fileMagic := string(header[0:6])

	// Extract version (2 bytes)
	fileVersion := string(header[6:8])

	if fileMagic != MainFileMagicString {
		return fmt.Errorf("invalid main database file format")
	}

	if fileVersion != VersionString {
		return fmt.Errorf("unsupported main database version")
	}

	// Extract database ID (8 bytes)
	db.databaseID = binary.LittleEndian.Uint64(header[8:16])

	return nil
}

// readIndexFileHeader reads the index file header
func (db *DB) readIndexFileHeader(finalRead bool) error {
	var header []byte
	var err error

	if finalRead {
		// Try to get the header page from the cache first (which includes pages from the WAL)
		headerPage, exists := db.getFromCache(0)
		if exists && headerPage != nil {
			header = headerPage.data
		}
	}
	// If not using WAL or if the page is not in cache
	if header == nil {
		// Read directly from the index file
		header, err = db.readFromIndexFile(0)
		if err != nil {
			return fmt.Errorf("failed to read index file header: %w", err)
		}
	}

	// Extract magic string (6 bytes)
	fileMagic := string(header[0:6])

	// Extract version (2 bytes)
	fileVersion := string(header[6:8])

	if fileMagic != IndexFileMagicString {
		return fmt.Errorf("invalid index database file format")
	}

	if fileVersion != VersionString {
		return fmt.Errorf("unsupported index database version")
	}

	// Extract database ID (8 bytes)
	indexDatabaseID := binary.LittleEndian.Uint64(header[8:16])

	// Check if the database ID matches the main file
	if db.databaseID != 0 && indexDatabaseID != db.databaseID {
		// Database ID mismatch, delete the index file and recreate it
		debugPrint("Index file database ID mismatch: %d vs %d, recreating index file\n", indexDatabaseID, db.databaseID)

		// Close the index file
		if err := db.indexFile.Close(); err != nil {
			return fmt.Errorf("failed to close index file: %w", err)
		}

		// Delete the index file
		if err := os.Remove(db.filePath + "-index"); err != nil {
			return fmt.Errorf("failed to delete index file: %w", err)
		}

		// Reopen the index file
		db.indexFile, err = os.OpenFile(db.filePath + "-index", os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			return fmt.Errorf("failed to reopen index file: %w", err)
		}

		// Initialize a new index file with the correct database ID
		return db.initializeIndexFile()
	}

	// Only process lastIndexedOffset on the final read
	if finalRead {
		// Parse the last indexed offset from the header
		db.lastIndexedOffset = int64(binary.LittleEndian.Uint64(header[16:24]))
		if db.lastIndexedOffset == 0 {
			// If the last indexed offset is 0, default to PageSize
			db.lastIndexedOffset = PageSize
		}

		// The array of hybrid pages with free space is parsed in parseHeaderPage
	} else {
		// Parse the number of main index pages from the header
		db.mainIndexPages = int(binary.LittleEndian.Uint32(header[24:28]))
	}

	return nil
}

// writeIndexHeader writes metadata to the index file header
func (db *DB) writeIndexHeader(isInit bool) error {
	// Don't update if database is in read-only mode
	if db.readOnly {
		return nil
	}

	// Handle initialization separately for clarity
	if isInit {
		return db.initializeIndexHeader()
	}

	// Check which sequence number to use
	var maxReadSeq int64
	db.seqMutex.Lock()
	if db.inTransaction {
		maxReadSeq = db.flushSequence
	} else {
		maxReadSeq = db.txnSequence
	}
	db.seqMutex.Unlock()

	// Get the header page from cache
	headerPage, err := db.getPage(0, maxReadSeq)
	if err != nil {
		return fmt.Errorf("failed to get header page: %w", err)
	}

	data := headerPage.data

	// Update the header fields
	lastIndexedOffset := db.flushFileSize

	// Set last indexed offset (8 bytes)
	binary.LittleEndian.PutUint64(data[16:24], uint64(lastIndexedOffset))

	// Serialize the free hybrid space array
	// Array count at offset 28 (2 bytes)
	arrayCount := len(headerPage.freeHybridSpaceArray)
	if arrayCount > MaxFreeSpaceEntries {
		arrayCount = MaxFreeSpaceEntries
	}
	binary.LittleEndian.PutUint16(data[28:30], uint16(arrayCount))

	// Serialize each entry (6 bytes each: 4 bytes page number + 2 bytes free space)
	for i := 0; i < arrayCount; i++ {
		offset := 30 + (i * 6)
		if offset+6 > len(data) {
			break // Avoid writing beyond page bounds
		}

		entry := headerPage.freeHybridSpaceArray[i]
		binary.LittleEndian.PutUint32(data[offset:offset+4], entry.PageNumber)
		binary.LittleEndian.PutUint16(data[offset+4:offset+6], entry.FreeSpace)
	}

	// Write the page to disk
	if err := db.writeIndexPage(headerPage); err != nil {
		return fmt.Errorf("failed to write index file header page: %w", err)
	}

	// Update the in-memory offset
	db.lastIndexedOffset = lastIndexedOffset

	return nil
}

// initializeIndexHeader creates and writes the initial index header
func (db *DB) initializeIndexHeader() error {
	// Allocate a buffer for the entire page
	data := make([]byte, PageSize)

	// Write the 6-byte magic string
	copy(data[0:6], IndexFileMagicString)

	// Write the 2-byte version
	copy(data[6:8], VersionString)

	// Write the 8-byte database ID
	binary.LittleEndian.PutUint64(data[8:16], db.databaseID)

	// Set initial last indexed offset (8 bytes)
	binary.LittleEndian.PutUint64(data[16:24], uint64(PageSize))

	// Store the number of main index pages (4 bytes)
	binary.LittleEndian.PutUint32(data[24:28], uint32(db.mainIndexPages))

	// Initialize empty free hybrid space array
	binary.LittleEndian.PutUint16(data[28:30], 0)

	// Set the file size to PageSize
	db.indexFileSize = PageSize

	// Create a Page struct for the header page
	headerPage := &Page{
		pageNumber:           0,
		data:                 data,
		freeHybridSpaceArray: make([]FreeSpaceEntry, 0, MaxFreeSpaceEntries),
	}

	// Set the transaction sequence number
	headerPage.txnSequence = db.txnSequence

	// Update the access time
	db.accessCounter++
	headerPage.accessTime = db.accessCounter

	// Mark the page as dirty
	db.markPageDirty(headerPage)

	// Add to cache
	db.addToCache(headerPage)

	// Write the page to disk
	if err := db.writeIndexPage(headerPage); err != nil {
		return fmt.Errorf("failed to write initial index file header page: %w", err)
	}

	// Set the last indexed offset
	db.lastIndexedOffset = PageSize

	return nil
}

// readExternalValues reads the external values from the files
func (db *DB) readExternalValues() error {
	// Search for all files with the pattern "<db-name>-vk-<key-in-hex>.<sequential-number>"
	pattern := db.filePath + "-vk-*"
	filePaths, err := filepath.Glob(pattern)
	if err != nil {
		return fmt.Errorf("failed to glob external values: %w", err)
	}

	// Group files by key and store all files for each key
	type fileInfo struct {
		fullPath string
		seqNum   int
	}
	keyFiles := make(map[string][]fileInfo)

	// Parse all files and group by key
	for _, filePath := range filePaths {
		fileName := filepath.Base(filePath)

		debugPrint("Processing file %s\n", fileName)

		// Remove the prefix to get the key and sequential number part
		prefixBase := filepath.Base(db.filePath) + "-vk-"
		remaining := fileName[len(prefixBase):]

		// Split by the last dot to separate key from sequential number
		lastDotIndex := strings.LastIndex(remaining, ".")
		if lastDotIndex == -1 {
			continue // Skip files without sequential number
		}

		keyHex := remaining[:lastDotIndex]
		seqNumStr := remaining[lastDotIndex+1:]

		// Parse sequential number
		seqNum, err := strconv.Atoi(seqNumStr)
		if err != nil {
			continue // Skip files with invalid sequential numbers
		}

		// Add file to the list for this key
		keyFiles[keyHex] = append(keyFiles[keyHex], fileInfo{
			fullPath: filePath,
			seqNum:   seqNum,
		})
	}

	// For each key, sort files in reverse order by sequential number and try reading them
	for keyHex, files := range keyFiles {
		// Sort files in reverse order by sequential number (highest first)
		sort.Slice(files, func(i, j int) bool {
			return files[i].seqNum > files[j].seqNum
		})

		// Convert the key to a byte array
		keyBytes, err := hex.DecodeString(keyHex)
		if err != nil {
			return fmt.Errorf("failed to decode external value key %s: %w", keyHex, err)
		}

		// Try reading files from highest to lowest sequential number until one succeeds
		var lastErr error
		for _, file := range files {
			fullPath := file.fullPath
			err = db.readExternalValue(keyBytes, fullPath, uint64(file.seqNum))
			if err == nil {
				lastErr = nil
				break // Successfully read the file, move to next key
			}
			lastErr = err // Keep track of the last error
		}

		// If none of the files could be read, return the last error
		if lastErr != nil {
			return fmt.Errorf("failed to read any external value file for key %s: %w", keyHex, lastErr)
		}
	}

	return nil
}

func (db *DB) readExternalValue(key []byte, fileName string, sequenceNumber uint64) error {
	// Check if the key already exists
	for _, extKey := range db.externalKeys {
		if bytes.Equal(extKey.key, key) {
			return nil
		}
	}

	// Read and parse the file
	value, _, recordCount, err := db.readExternalValueFile(fileName)
	if err != nil {
		return fmt.Errorf("failed to read external value: %w", err)
	}

	// Create the value entry
	valueEntry := &externalValueEntry{
		value: value,
		txnSequence: 1,  //txnSeq,
		dirty: false,
		next: nil,
	}
	// Add the external key with file info tracking
	db.externalKeys = append(db.externalKeys, &externalKey{
		key: key,
		value: valueEntry,
		sequenceNumber: sequenceNumber,
		recordCount: recordCount,
	})

	return nil
}

// readExternalValueFile reads the latest value from a external value file
func (db *DB) readExternalValueFile(fileName string) ([]byte, int64, int, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("failed to open file %s: %w", fileName, err)
	}
	defer file.Close()

	// Get file info to calculate record count
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, 0, 0, fmt.Errorf("failed to get file info: %w", err)
	}

	fileSize := fileInfo.Size()
	recordCount := 0
	var latestValue []byte
	var latestTxnSeq int64 = -1
	var validEndOffset int64 = 0

	// Read all records and find the latest one (highest txnSequence)
	offset := int64(0)
	for offset < fileSize {
		// Read header (16 bytes)
		header := make([]byte, 16)
		n, err := file.ReadAt(header, offset)
		if err != nil || n != 16 {
			debugPrint("Error reading header: %v\n", err)
			break // End of file or corrupted header
		}

		// Parse header: checksum (4 bytes) + contentSize (4 bytes) + txnSequence (8 bytes)
		expectedChecksum := binary.LittleEndian.Uint32(header[0:4])
		contentSize := binary.LittleEndian.Uint32(header[4:8])
		txnSequence := int64(binary.LittleEndian.Uint64(header[8:16]))

		// Read value
		value := make([]byte, contentSize)
		n, err = file.ReadAt(value, offset+16)
		if err != nil || uint32(n) != contentSize {
			debugPrint("Error reading value: %v\n", err)
			break // End of file or corrupted data
		}

		// Verify checksum using incremental approach
		checksum := crc32.NewIEEE()
		checksum.Write(header[4:]) // contentSize + txnSequence (12 bytes)
		checksum.Write(value)      // value data
		actualChecksum := checksum.Sum32()
		if actualChecksum != expectedChecksum {
			debugPrint("Corrupted record found in file %s\n", fileName)
			// Corrupted record found, stop reading
			break
		}

		// Keep track of the latest record (highest txnSequence)
		//if txnSequence > latestTxnSeq {
			latestTxnSeq = txnSequence
			latestValue = value
		//}

		recordCount++
		offset += 16 + int64(contentSize)
		validEndOffset = offset // Update valid end offset
	}

	// If we stopped before the end of file due to corruption and not in read-only mode, truncate
	if validEndOffset < fileSize && !db.readOnly {
		debugPrint("Truncating file %s\n", fileName)
		file.Close() // Close read-only file first

		// Reopen for writing and truncate
		writeFile, err := os.OpenFile(fileName, os.O_WRONLY, 0644)
		if err != nil {
			return nil, 0, 0, fmt.Errorf("failed to open file for truncation %s: %w", fileName, err)
		}
		if err := writeFile.Truncate(validEndOffset); err != nil {
			writeFile.Close()
			return nil, 0, 0, fmt.Errorf("failed to truncate corrupted file %s: %w", fileName, err)
		}
		writeFile.Close()
	}

	// If no valid records found, return empty value
	if latestTxnSeq == -1 {
		return []byte{}, 0, recordCount, nil
	}

	return latestValue, latestTxnSeq, recordCount, nil
}

// writeExternalValues writes dirty external values to disk
func (db *DB) writeExternalValues() error {
	if db.readOnly {
		return nil // Skip writing in read-only mode
	}

	debugPrint("Writing external values - flushSequence: %d\n", db.flushSequence)

	for _, extKey := range db.externalKeys {
		// Find the first dirty entry that should be written
		entry := extKey.value

		for entry != nil {
			// Only write if dirty and txnSequence <= flushSequence
			if entry.dirty && entry.txnSequence <= db.flushSequence {
				// Check if we need to rotate to a new file (>= 512 records)
				if extKey.recordCount >= 512 {
					// Close current file before rotating
					if extKey.openFile != nil {
						extKey.openFile.Close()
						extKey.openFile = nil
					}
					// Rotate to a new file
					extKey.sequenceNumber++
					extKey.recordCount = 0
					// Clean up old files (keep only last 5)
					if extKey.sequenceNumber >= 5 {
						keyHex := hex.EncodeToString(extKey.key)
						oldSeqNum := extKey.sequenceNumber - 5
						oldFileName := fmt.Sprintf("%s-vk-%s.%d", db.filePath, keyHex, oldSeqNum)
						os.Remove(oldFileName) // Ignore errors for non-existent files
					}
				}

				// Open file if not already open
				if extKey.openFile == nil {
					keyHex := hex.EncodeToString(extKey.key)
					fileName := fmt.Sprintf("%s-vk-%s.%d", db.filePath, keyHex, extKey.sequenceNumber)
					file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
					if err != nil {
						return fmt.Errorf("failed to open file %s: %w", fileName, err)
					}
					extKey.openFile = file
				}

				// Write the value to the file
				err := db.writeExternalValueToOpenFile(*entry, extKey.openFile)
				if err != nil {
					return fmt.Errorf("failed to write external value for key %x: %w", extKey.key, err)
				}

				// Update file info
				extKey.recordCount++

				// Mark as not dirty
				entry.dirty = false
				// Clean up old versions (discard entries after the written entry)
				entry.next = nil
				// Write only one value per key
				break
			}
			entry = entry.next
		}
	}

	return nil
}

// writeExternalValueToOpenFile writes a single external value to the specified file
func (db *DB) writeExternalValueToOpenFile(entry externalValueEntry, file *os.File) error {
	// Calculate content size
	contentSize := uint32(len(entry.value))

	// Create header: checksum (4 bytes) + contentSize (4 bytes) + txnSequence (8 bytes) = 16 bytes
	header := make([]byte, 16)
	// Fill in contentSize and txnSequence first (skip checksum at bytes 0-3)
	binary.LittleEndian.PutUint32(header[4:8], contentSize)
	binary.LittleEndian.PutUint64(header[8:16], uint64(entry.txnSequence))

	// Calculate CRC32 checksum using incremental approach
	checksumCalc := crc32.NewIEEE()
	checksumCalc.Write(header[4:]) // contentSize + txnSequence (12 bytes)
	checksumCalc.Write(entry.value) // value data
	checksum := checksumCalc.Sum32()

	// Fill in the checksum
	binary.LittleEndian.PutUint32(header[0:4], checksum)

	// Write header
	if _, err := file.Write(header); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	// Write value
	if _, err := file.Write(entry.value); err != nil {
		return fmt.Errorf("failed to write value: %w", err)
	}

	if db.syncMode == SyncOn {
		// Sync to ensure data is written to disk
		if err := file.Sync(); err != nil {
			return fmt.Errorf("failed to sync file: %w", err)
		}
	}

	return nil
}

// closeExternalFiles closes all open external value files
func (db *DB) closeExternalFiles() {
	for _, extKey := range db.externalKeys {
		if extKey.openFile != nil {
			extKey.openFile.Close()
			extKey.openFile = nil
		}
	}
}

// ------------------------------------------------------------------------------------------------
// Main file
// ------------------------------------------------------------------------------------------------

// appendData appends a key-value pair to the end of the file and returns its offset
func (db *DB) appendData(key, value []byte) (int64, error) {
	// Use stored file size to determine where to append
	fileSize := db.mainFileSize

	// Calculate the total size needed
	keyLenSize := varint.Size(uint64(len(key)))
	valueLenSize := varint.Size(uint64(len(value)))
	totalSize := 1 + keyLenSize + valueLenSize + len(key) + len(value) // 1 byte for content type

	// Prepare the content buffer
	content := make([]byte, totalSize)
	offset := 0

	// Write content type
	content[offset] = ContentTypeData
	offset++

	// Write key length
	keyLenWritten := varint.Write(content[offset:], uint64(len(key)))
	offset += keyLenWritten

	// Write value length
	valueLenWritten := varint.Write(content[offset:], uint64(len(value)))
	offset += valueLenWritten

	// Write key
	copy(content[offset:], key)
	offset += len(key)

	// Write value
	copy(content[offset:], value)

	// Write the content to the end of the file
	if _, err := db.mainFile.Write(content); err != nil {
		return 0, fmt.Errorf("failed to write content: %w", err)
	}

	// Update the running transaction checksum
	db.txnChecksum = crc32.Update(db.txnChecksum, crc32.IEEETable, content)

	// Update the file size
	db.mainFileSize += int64(totalSize)

	debugPrint("Appended content at offset %d, size %d\n", fileSize, totalSize)

	// Cache the newly written value for faster future reads
	db.addToValueCache(fileSize, key, value)

	// Return the offset where the content was written
	return fileSize, nil
}

// appendCommitMarker appends a commit marker to the end of the main file
// The commit marker consists of:
// - 1 byte: ContentTypeCommit ('C')
// - 4 bytes: CRC32 checksum of all transaction data since the last commit
func (db *DB) appendCommitMarker() error {
	// Use the running transaction checksum
	checksum := db.txnChecksum

	// Prepare the commit marker buffer (1 byte type + 4 bytes checksum)
	commitMarker := make([]byte, 5)
	commitMarker[0] = ContentTypeCommit
	binary.BigEndian.PutUint32(commitMarker[1:5], checksum)

	// Write the commit marker to the end of the file
	if _, err := db.mainFile.Write(commitMarker); err != nil {
		return fmt.Errorf("failed to write commit marker: %w", err)
	}

	// Update the file size
	db.mainFileSize += 5

	// Reset the transaction checksum for the next transaction
	db.txnChecksum = 0

	debugPrint("Appended commit marker at offset %d with checksum %d\n", db.mainFileSize-5, checksum)

	return nil
}

// readContentValue reads just the value from content at a specific offset, using cache when possible
func (db *DB) readContentValue(offset int64, key []byte) ([]byte, error) {
	// Check if offset is valid
	if offset < 0 || offset >= db.mainFileSize {
		return nil, fmt.Errorf("offset out of file bounds: %d", offset)
	}

	// Try to get from value cache first
	if cachedValue, found := db.getFromValueCache(offset, key); found {
		return cachedValue, nil
	}

	// Not in cache, read the content from disk
	content, err := db.readContent(offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read content: %w", err)
	}

	// Verify that the key matches
	if !equal(content.key, key) {
		// It is a collision: both keys map to the same path in the hash-table tree
		return nil, fmt.Errorf("key not found")
	}

	// Cache the value for future reads
	db.addToValueCache(offset, key, content.value)

	// Return the value
	return content.value, nil
}

// readContent reads content from a specific offset in the file
func (db *DB) readContent(offset int64) (*Content, error) {
	// Check if offset is valid
	if offset < 0 || offset >= db.mainFileSize {
		return nil, fmt.Errorf("offset out of file bounds: %d", offset)
	}

	content := &Content{
		offset: offset,
	}

	// Read 1: Read 19 bytes, sufficient for type + 2 varints (key size + value size)
	buffer := make([]byte, 19)
	n, err := db.mainFile.ReadAt(buffer, offset)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("failed to read content header: %w", err)
	}

	if n < 1 {
		return nil, fmt.Errorf("failed to read content type")
	}

	contentType := buffer[0]

	if contentType == ContentTypeData {
		// Parse key length
		keyLengthOffset := 1 // Skip content type byte
		keyLength64, keyBytesRead := varint.Read(buffer[keyLengthOffset:])
		if keyBytesRead == 0 {
			return nil, fmt.Errorf("failed to parse key length")
		}
		if keyLength64 > MaxKeyLength {
			return nil, fmt.Errorf("key length exceeds maximum allowed size: %d", keyLength64)
		}
		keyLength := int(keyLength64)

		// Parse value length
		valueLengthOffset := keyLengthOffset + keyBytesRead
		valueLength64, valueBytesRead := varint.Read(buffer[valueLengthOffset:])
		if valueBytesRead == 0 {
			return nil, fmt.Errorf("failed to parse value length")
		}
		valueLength := int(valueLength64)

		if valueLength > MaxValueLength {
			return nil, fmt.Errorf("value length exceeds maximum allowed size: %d", valueLength)
		}

		// Calculate offsets and total size
		keyOffset := valueLengthOffset + valueBytesRead
		valueOffset := keyOffset + keyLength
		totalSize := valueOffset + valueLength

		// Check if total size exceeds file size
		if offset + int64(totalSize) > db.mainFileSize {
			return nil, fmt.Errorf("content extends beyond file size")
		}

		// Read 2: Read the entire content
		buffer = make([]byte, totalSize)
		n, err = db.mainFile.ReadAt(buffer, offset)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("failed to read content: %w", err)
		}

		// Make sure we got all the data
		if n < totalSize {
			return nil, fmt.Errorf("failed to read complete content data")
		}

		// Store the full data buffer
		content.data = buffer

		// Set key and value as slices that reference the original buffer
		content.key = buffer[keyOffset:keyOffset+keyLength]
		content.value = buffer[valueOffset:valueOffset+valueLength]

	} else if contentType == ContentTypeCommit {
		// No need to read again, we already have 19 bytes which is sufficient for 5 bytes commit marker
		if n < 5 {
			return nil, fmt.Errorf("incomplete commit marker")
		}
		// Store the commit marker data (reuse buffer, just take first 5 bytes)
		content.data = buffer[:5]

	} else {
		return nil, fmt.Errorf("unknown content type on main file: %c", contentType)
	}

	return content, nil
}

// ------------------------------------------------------------------------------------------------
// Header page
// ------------------------------------------------------------------------------------------------

func (db *DB) parseHeaderPage(data []byte) (*Page, error) {

	// Parse the free hybrid space array
	// Array count is stored at offset 28 (2 bytes)
	arrayCount := int(binary.LittleEndian.Uint16(data[28:30]))

	// Initialize the array with the correct capacity
	freeHybridSpaceArray := make([]FreeSpaceEntry, 0, MaxFreeSpaceEntries)

	// Parse each entry (6 bytes each: 4 bytes page number + 2 bytes free space)
	for i := 0; i < arrayCount && i < MaxFreeSpaceEntries; i++ {
		offset := 30 + (i * 6)
		if offset+6 > len(data) {
			break // Avoid reading beyond header bounds
		}

		pageNumber := binary.LittleEndian.Uint32(data[offset:offset+4])
		freeSpace := binary.LittleEndian.Uint16(data[offset+4:offset+6])

		// Only add valid entries
		if pageNumber > 0 {
			freeHybridSpaceArray = append(freeHybridSpaceArray, FreeSpaceEntry{
				PageNumber: pageNumber,
				FreeSpace:  freeSpace,
			})
		}
	}

	// Create the header page
	headerPage := &Page{
		pageNumber: 0,
		data:       data,
		freeHybridSpaceArray: freeHybridSpaceArray,
		txnSequence: db.txnSequence,
	}

	// Update the access time
	db.accessCounter++
	headerPage.accessTime = db.accessCounter

	return headerPage, nil
}

// ------------------------------------------------------------------------------------------------
// Table pages
// ------------------------------------------------------------------------------------------------

// parseTablePage parses a table page read from the disk
func (db *DB) parseTablePage(data []byte, pageNumber uint32) (*TablePage, error) {
	// Check if it's a table page
	if data[4] != ContentTypeTable {
		return nil, fmt.Errorf("not a table page at page %d", pageNumber)
	}

	// Verify CRC32 checksum
	storedChecksum := binary.BigEndian.Uint32(data[0:4])
	// Calculate the checksum
	calculatedChecksum := crc32.ChecksumIEEE(data[4:])
	// Verify the checksum
	if storedChecksum != calculatedChecksum {
		return nil, fmt.Errorf("table page checksum mismatch at page %d: stored=%d, calculated=%d", pageNumber, storedChecksum, calculatedChecksum)
	}

	// Read the salt
	salt := data[5]

	// Create structured table page
	tablePage := &TablePage{
		pageNumber: pageNumber,
		pageType:   ContentTypeTable,
		data:       data,
		dirty:      false,
		Salt:       salt,
	}

	// Update the access time
	db.accessCounter++
	tablePage.accessTime = db.accessCounter

	return tablePage, nil
}

// writeTablePage writes a table page to the database file
func (db *DB) writeTablePage(tablePage *TablePage) error {
	// Set page type and salt in the data
	tablePage.data[4] = ContentTypeTable  // Type identifier
	tablePage.data[5] = tablePage.Salt    // Salt for hash table

	// Calculate CRC32 checksum for the page data (excluding the checksum field itself)
	checksum := crc32.ChecksumIEEE(tablePage.data[4:])
	// Write the checksum at position 0
	binary.BigEndian.PutUint32(tablePage.data[0:4], checksum)

	// Ensure the page number and offset are valid
	if tablePage.pageNumber == 0 {
		return fmt.Errorf("cannot write table page with page number 0")
	}

	debugPrint("Writing table page to index file at page %d\n", tablePage.pageNumber)

	// Write to disk at the specified page number
	return db.writeIndexPage(tablePage)
}

// ------------------------------------------------------------------------------------------------
// Hybrid pages
// ------------------------------------------------------------------------------------------------

// parseHybridPage parses a hybrid page read from the disk
func (db *DB) parseHybridPage(data []byte, pageNumber uint32) (*HybridPage, error) {
	// Check if it's a hybrid page
	if data[4] != ContentTypeHybrid {
		return nil, fmt.Errorf("not a hybrid page at page %d", pageNumber)
	}

	// Verify CRC32 checksum
	storedChecksum := binary.BigEndian.Uint32(data[0:4])
	// Calculate the checksum
	calculatedChecksum := crc32.ChecksumIEEE(data[4:])
	// Verify the checksum
	if storedChecksum != calculatedChecksum {
		return nil, fmt.Errorf("hybrid page checksum mismatch at page %d: stored=%d, calculated=%d", pageNumber, storedChecksum, calculatedChecksum)
	}

	// Get number of sub-pages and content size
	numSubPages := data[5]
	contentSize := int(binary.LittleEndian.Uint16(data[6:8]))

	// Create structured hybrid page
	hybridPage := &HybridPage{
		pageNumber:  pageNumber,
		pageType:    ContentTypeHybrid,
		data:        data,
		dirty:       false,
		NumSubPages: numSubPages,
		ContentSize: contentSize,
		SubPages:    make([]HybridSubPageInfo, 128),
	}

	// Parse sub-page offsets and entries
	if err := db.parseHybridSubPages(hybridPage); err != nil {
		return nil, fmt.Errorf("failed to parse hybrid sub-pages: %w", err)
	}

	// Update the access time
	db.accessCounter++
	hybridPage.accessTime = db.accessCounter

	return hybridPage, nil
}

// parseHybridSubPages parses the sub-pages in a hybrid page (metadata only, not entries)
func (db *DB) parseHybridSubPages(hybridPage *HybridPage) error {
	// Start at header size
	pos := HybridHeaderSize

	// If there are no sub-pages, nothing to do
	if hybridPage.ContentSize == HybridHeaderSize {
		return nil
	}

	// Read each sub-page until we reach the content size
	for pos < hybridPage.ContentSize {
		// Read sub-page ID
		subPageID := hybridPage.data[pos]
		pos++

		// Read sub-page salt
		subPageSalt := hybridPage.data[pos]
		pos++

		// Read sub-page size
		subPageSize := binary.LittleEndian.Uint16(hybridPage.data[pos:pos+2])
		pos += 2

		// Calculate the end of this sub-page
		subPageEnd := pos + int(subPageSize)

		// Ensure we don't read past the content size
		if subPageEnd > hybridPage.ContentSize {
			return fmt.Errorf("sub-page end %d exceeds content size %d", subPageEnd, hybridPage.ContentSize)
		}

		// Create and store the sub-page info at the index corresponding to its ID
		hybridPage.SubPages[subPageID] = HybridSubPageInfo{
			Salt:   subPageSalt,
			Offset: uint16(pos - 4), // Include the header in the offset
			Size:   subPageSize,
		}

		// Move to the next sub-page
		pos = subPageEnd
	}

	return nil
}

// iterateHybridSubPageEntries iterates through entries in a hybrid sub-page, calling the callback for each entry
// The callback receives entryOffset, entrySize, slot, isSubPage, and value
// Returns true to continue or false to stop
func (db *DB) iterateHybridSubPageEntries(hybridPage *HybridPage, SubPageId uint8, callback func(entryOffset int, entrySize int, slot int, isSubPage bool, value uint64) bool) error {
	// Get the sub-page info
	if int(SubPageId) >= len(hybridPage.SubPages) || hybridPage.SubPages[SubPageId].Offset == 0 {
		return fmt.Errorf("sub-page with index %d not found", SubPageId)
	}
	subPageInfo := &hybridPage.SubPages[SubPageId]

	// Calculate the start and end positions for this sub-page's data
	subPageDataStart := int(subPageInfo.Offset) + HybridSubPageHeaderSize // Skip 4-byte header (ID + Salt + Size)
	subPageDataEnd := subPageDataStart + int(subPageInfo.Size)

	pos := subPageDataStart

	// Read entries until we reach the end of the sub-page
	for pos < subPageDataEnd {
		// Store the entry offset relative to the page data
		entryOffset := pos

		// Read slot/position (varint)
		slot64, bytesRead := varint.Read(hybridPage.data[pos:])
		if bytesRead == 0 {
			return fmt.Errorf("failed to read slot/position")
		}
		slot := int(slot64)
		pos += bytesRead

		// Check if we have at least one more byte for the type indicator
		if pos >= subPageDataEnd {
			return fmt.Errorf("insufficient space for type indicator")
		}

		// Check the first bit of the next byte to determine the type
		typeByte := hybridPage.data[pos]
		isSubPage := (typeByte & 0x80) == 0 // First bit is 0 = sub-page pointer

		var value uint64
		var entrySize int
		if isSubPage {
			// Sub-page pointer: 5 bytes (1 byte sub-page id + 4 bytes page number)
			if pos+5 > subPageDataEnd {
				return fmt.Errorf("insufficient space for sub-page pointer")
			}

			// Read 5 bytes for sub-page pointer
			subPageId := hybridPage.data[pos]
			pageNumber := binary.LittleEndian.Uint32(hybridPage.data[pos+1:pos+5])
			// Combine sub-page ID and page number into value
			value = uint64(subPageId) | (uint64(pageNumber) << 8)
			pos += 5
			entrySize = bytesRead + 5 // slot varint + 5 bytes for sub-page pointer
		} else {
			// Data offset: 8 bytes with first bit set to 1
			if pos+8 > subPageDataEnd {
				return fmt.Errorf("insufficient space for data offset")
			}

			// Read 8 bytes for data offset (using big-endian)
			value = binary.BigEndian.Uint64(hybridPage.data[pos:pos+8])
			// Clear the first bit to get the actual offset
			value &= 0x7FFFFFFFFFFFFFFF
			pos += 8
			entrySize = bytesRead + 8 // slot varint + 8 bytes for data offset
		}

		// Call the callback with the entry information (matching original pattern)
		if !callback(entryOffset, entrySize, slot, isSubPage, value) {
			break
		}
	}

	return nil
}

// findEntryInHybridSubPage finds an entry in a hybrid sub-page for the given slot
// Returns entryOffset, entrySize, whether it's a sub-page pointer, and the value if found
// Returns 0, 0, false, 0 if not found
func (db *DB) findEntryInHybridSubPage(hybridPage *HybridPage, SubPageId uint8, targetSlot int) (int, int, bool, uint64, error) {
	// Get the sub-page info
	if int(SubPageId) >= len(hybridPage.SubPages) || hybridPage.SubPages[SubPageId].Offset == 0 {
		return 0, 0, false, 0, fmt.Errorf("sub-page with index %d not found", SubPageId)
	}

	var foundEntryOffset int = 0
	var foundEntrySize int = 0
	var foundIsSubPage bool = false
	var foundValue uint64 = 0

	err := db.iterateHybridSubPageEntries(hybridPage, SubPageId, func(entryOffset int, entrySize int, slot int, isSubPage bool, value uint64) bool {
		if slot == targetSlot {
			foundEntryOffset = entryOffset
			foundEntrySize = entrySize
			foundIsSubPage = isSubPage
			foundValue = value
			return false // Stop iteration
		}

		return true // Continue iteration
	})

	if foundValue == 0 {
		return 0, 0, false, 0, err
	}

	return foundEntryOffset, foundEntrySize, foundIsSubPage, foundValue, err
}

// writeHybridPage writes a hybrid page to the database file
func (db *DB) writeHybridPage(hybridPage *HybridPage) error {
	// Set page type, number of sub-pages, and content size in the data
	hybridPage.data[4] = ContentTypeHybrid  // Type identifier
	hybridPage.data[5] = hybridPage.NumSubPages  // Number of sub-pages on this page
	binary.LittleEndian.PutUint16(hybridPage.data[6:8], uint16(hybridPage.ContentSize))

	// Calculate CRC32 checksum for the page data (excluding the checksum field itself)
	checksum := crc32.ChecksumIEEE(hybridPage.data[4:])
	// Write the checksum at position 0
	binary.BigEndian.PutUint32(hybridPage.data[0:4], checksum)

	// Ensure the page number and offset are valid
	if hybridPage.pageNumber == 0 {
		return fmt.Errorf("cannot write hybrid page with page number 0")
	}

	debugPrint("Writing hybrid page to index file at page %d\n", hybridPage.pageNumber)

	// Write to disk at the specified page number
	return db.writeIndexPage(hybridPage)
}

// ------------------------------------------------------------------------------------------------
// Index pages
// ------------------------------------------------------------------------------------------------

// readPage reads a page from the index file
// first read 1 byte to check the page type
// then read the page data
func (db *DB) readPage(pageNumber uint32) (*Page, error) {
	// Read the page data
	data, err := db.readFromIndexFile(pageNumber)
	if err != nil {
		return nil, fmt.Errorf("failed to read page: %w", err)
	}

	// If the page number is 0, it's a header page
	if pageNumber == 0 {
		return db.parseHeaderPage(data)
	}

	// Get the page type
	contentType := data[4]

	// Parse the page based on the page type (or page number for header page)
	if contentType == ContentTypeTable {
		return db.parseTablePage(data, pageNumber)
	} else if contentType == ContentTypeHybrid {
		return db.parseHybridPage(data, pageNumber)
	}

	return nil, fmt.Errorf("unknown page type: %c", contentType)
}

// writeIndexPage writes an index page to either the WAL file or the index file
func (db *DB) writeIndexPage(page *Page) error {
	var err error

	// If WAL is used, write to WAL file
	if db.useWAL {
		err = db.writeToWAL(page.data, page.pageNumber)
	// Otherwise, write directly to the index file
	} else {
		err = db.writeToIndexFile(page.data, page.pageNumber)
	}

	// If the page was written successfully
	if err == nil {
		// Mark it as clean
		db.markPageClean(page)
		// If using WAL, mark it as part of the WAL
		if db.useWAL {
			page.isWAL = true
		}
		// Discard previous versions of this page
		bucket := &db.pageCache[page.pageNumber & 1023]
		bucket.mutex.Lock()
		page.next = nil
		bucket.mutex.Unlock()
	}

	return err
}

// writeToIndexFile writes an index page to the index file
func (db *DB) writeToIndexFile(data []byte, pageNumber uint32) error {
	// Calculate file offset from page number
	offset := int64(pageNumber) * PageSize

	// Check if offset is valid
	if offset < 0 || offset >= db.indexFileSize {
		return fmt.Errorf("page number %d out of index file bounds", pageNumber)
	}

	// Write the page data
	if _, err := db.indexFile.WriteAt(data, offset); err != nil {
		return fmt.Errorf("failed to write page data to index file: %w", err)
	}

	return nil
}

// readFromIndexFile reads an index page from the index file
func (db *DB) readFromIndexFile(pageNumber uint32) ([]byte, error) {
	// Calculate file offset from page number
	offset := int64(pageNumber) * PageSize

	// Check if offset is valid
	if offset < 0 || offset >= db.indexFileSize {
		return nil, fmt.Errorf("page number %d out of index file bounds", pageNumber)
	}

	data := make([]byte, PageSize)

	// Read the page data
	if _, err := db.indexFile.ReadAt(data, offset); err != nil {
		return nil, fmt.Errorf("failed to read page data from index file: %w", err)
	}

	return data, nil
}

// ------------------------------------------------------------------------------------------------
// ...
// ------------------------------------------------------------------------------------------------

// Sync flushes all dirty pages to disk and syncs the files
func (db *DB) Sync() error {
	// Check if file is opened in read-only mode
	if db.readOnly {
		return fmt.Errorf("cannot sync: database opened in read-only mode")
	}

	// Coordinate with Close() to ensure files aren't closed during sync
	db.readMutex.RLock()
	defer db.readMutex.RUnlock()

	// Check if database is closed
	if db.isClosed {
		return fmt.Errorf("database is closed")
	}

	// Flush the index to disk
	if err := db.flushIndexToDisk(); err != nil {
		return fmt.Errorf("failed to flush index to disk: %w", err)
	}

	// Sync the main file
	if err := db.mainFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync main file: %w", err)
	}

	// Sync the index file
	if err := db.indexFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync index file: %w", err)
	}

	return nil
}

// refreshFileSize updates the cached file sizes from the actual files
func (db *DB) refreshFileSize() error {
	// Refresh main file size
	mainFileInfo, err := db.mainFile.Stat()
	if err != nil {
		return fmt.Errorf("failed to get main file size: %w", err)
	}
	db.mainFileSize = mainFileInfo.Size()

	// Refresh index file size
	indexFileInfo, err := db.indexFile.Stat()
	if err != nil {
		return fmt.Errorf("failed to get index file size: %w", err)
	}
	db.indexFileSize = indexFileInfo.Size()

	return nil
}

// ------------------------------------------------------------------------------------------------
// Transaction API
// ------------------------------------------------------------------------------------------------

// Begin a new transaction
func (db *DB) Begin() (*Transaction, error) {
	db.writeMutex.Lock()
	defer db.writeMutex.Unlock()

	// Wait if a transaction is already open
	for db.inExplicitTransaction {
		db.transactionCond.Wait()
	}

	// Check if database is closed
	if db.isClosed {
		return nil, fmt.Errorf("the database is closed")
	}

	// Mark transaction as open
	db.inExplicitTransaction = true

	// Start a transaction
	err := db.beginTransaction()
	if err != nil {
		return nil, err
	}

	// Create the transaction object
	tx := &Transaction{db: db, txnSequence: db.txnSequence}

	// Set a finalizer to rollback the transaction if it is not committed or rolled back
	runtime.SetFinalizer(tx, func(t *Transaction) {
		t.db.writeMutex.Lock()
		defer t.db.writeMutex.Unlock()
		if t.db.inExplicitTransaction && t.txnSequence == t.db.txnSequence {
			_ = t.rollback()
		}
	})

	// Return the transaction object
	return tx, nil
}

// Commit a transaction
func (tx *Transaction) Commit() error {
	tx.db.writeMutex.Lock()
	defer tx.db.writeMutex.Unlock()

	// Check if transaction is open
	if !tx.db.inExplicitTransaction {
		return fmt.Errorf("no transaction is open")
	}

	// Check if database is closed
	if tx.db.isClosed {
		return fmt.Errorf("database is closed")
	}

	// Commit the transaction
	tx.db.commitTransaction()

	// Mark transaction as closed
	tx.db.inExplicitTransaction = false

	// Signal waiting transactions
	tx.db.transactionCond.Signal()

	return nil
}

// Rollback a transaction
func (tx *Transaction) Rollback() error {
	// Check if database is closed
	if tx.db.isClosed {
		return nil
	}

	tx.db.writeMutex.Lock()
	defer tx.db.writeMutex.Unlock()

	return tx.rollback()
}

// rollback is the internal rollback function that assumes the mutex is already held
func (tx *Transaction) rollback() error {
	// Check if transaction is open
	if !tx.db.inExplicitTransaction {
		return fmt.Errorf("no transaction is open")
	}

	// Rollback the transaction
	tx.db.rollbackTransaction()

	// Mark transaction as closed
	tx.db.inExplicitTransaction = false

	// Signal waiting transactions
	tx.db.transactionCond.Signal()

	return nil
}

// Set a key-value pair within a transaction
func (tx *Transaction) Set(key, value []byte) error {
	// Call the database's set method
	return tx.db.set(key, value, true)
}

// Get a value for a key within a transaction
func (tx *Transaction) Get(key []byte) ([]byte, error) {
	// Set the flag to indicate this is called from a transaction
	return tx.db.get(key, true)
}

// Delete a key within a transaction
func (tx *Transaction) Delete(key []byte) error {
	// Call the database's delete method
	return tx.db.set(key, nil, true)
}

// ------------------------------------------------------------------------------------------------
// Internal Transactions
// ------------------------------------------------------------------------------------------------

// beginTransaction starts a new transaction
func (db *DB) beginTransaction() error {

	// If not already exclusively locked, acquire write lock for transaction
	if db.lockType != LockExclusive {
		// Remember the original lock type
		db.originalLockType = db.lockType
		// Acquire a write lock
		if err := db.acquireWriteLock(); err != nil {
			// This is a critical error that should be handled by the caller
			return fmt.Errorf("failed to acquire write lock for transaction: %w", err)
		}
		db.lockAcquiredForTransaction = true
	}

	db.seqMutex.Lock()

	// Mark the database as in a transaction
	db.inTransaction = true

	// Increment the transaction sequence number (to track the pages used in the transaction)
	db.txnSequence++

	// For fast rollback and slower writes, clone pages at every transaction
	if db.fastRollback {
		db.cloningSequence = db.txnSequence - 1
	// For fast writes and slow rollback, clone pages at every 1000 transactions
	} else {
		// If the page is marked to be flushed, we cannot modify its data (force cloning)
		if db.cloningSequence < db.flushSequence {
			db.cloningSequence = db.flushSequence
		}
		if db.txnSequence > db.cloningSequence + 1000 {
			db.cloningSequence = db.txnSequence - 1
			db.cloningFileSize = db.mainFileSize
		}
	}

	// Store the current main file size to enable rollback (to truncate the main file)
	db.prevFileSize = db.mainFileSize

	// Reset the transaction checksum
	db.txnChecksum = 0

	db.seqMutex.Unlock()


	// Get the header page
	headerPage, err := db.getPage(0)
	if err != nil {
		debugPrint("Failed to get header page: %v\n", err)
		return fmt.Errorf("failed to get header page: %w", err)
	}

	// Get a writable version of the header page
	headerPage, err = db.getWritablePage(headerPage)
	if err != nil {
		debugPrint("Failed to get writable header page: %v\n", err)
		return fmt.Errorf("failed to get writable header page: %w", err)
	}

	// Initialize the array if needed
	if headerPage.freeHybridSpaceArray == nil {
		headerPage.freeHybridSpaceArray = make([]FreeSpaceEntry, 0, MaxFreeSpaceEntries)
	}

	// Mark the header page as dirty
	db.markPageDirty(headerPage)

	// Store the header page for transaction
	db.headerPageForTransaction = headerPage

	debugPrint("Beginning transaction %d\n", db.txnSequence)
	return nil
}

// commitTransaction commits the current transaction
func (db *DB) commitTransaction() {
	debugPrint("Committing transaction %d\n", db.txnSequence)

	// Write commit marker to the main file if data was written in this transaction
	if !db.readOnly && db.mainFileSize > db.prevFileSize {
		if err := db.appendCommitMarker(); err != nil {
			debugPrint("Failed to write commit marker: %v\n", err)
			// Continue with commit even if marker fails
		}
	}

	// Release transaction lock if it was acquired for this transaction
	if db.lockAcquiredForTransaction {
		if err := db.releaseWriteLock(db.originalLockType); err != nil {
			debugPrint("Failed to release transaction lock: %v\n", err)
		}
		db.lockAcquiredForTransaction = false
	}

	db.seqMutex.Lock()
	db.inTransaction = false
	db.seqMutex.Unlock()

	// If in caller thread mode, flush to disk
	if db.commitMode == CallerThread {
		db.flushIndexToDisk()
	}
}

// rollbackTransaction rolls back the current transaction
func (db *DB) rollbackTransaction() {
	debugPrint("Rolling back transaction %d\n", db.txnSequence)

	// Truncate the main db file to the stored size before the transaction started
	if db.mainFileSize > db.prevFileSize {
		err := db.mainFile.Truncate(db.prevFileSize)
		if err != nil {
			debugPrint("Failed to truncate main file: %v\n", err)
		} else {
			debugPrint("Truncated main file to size %d\n", db.prevFileSize)
		}
		// Update the in-memory file size
		db.mainFileSize = db.prevFileSize

		// Invalidate cached values for offsets that are now beyond the file end
		db.invalidateValueCacheFromOffset(db.prevFileSize)
	}

	if db.fastRollback {
		// Fast rollback: discard pages from this transaction only
		db.discardNewerPages(db.txnSequence)
	} else {
		// Slow rollback: discard pages newer than the cloning mark and reindex
		db.discardNewerPages(db.cloningSequence + 1)

		// Get the last indexed offset
		lastIndexedOffset := db.cloningFileSize

		// Reindex the new content from the main file (incremental reindexing)
		if lastIndexedOffset < db.mainFileSize {
			err := db.reindexContent(lastIndexedOffset)
			if err != nil {
				debugPrint("Failed to reindex content during rollback: %v\n", err)
			}
		}
	}

	// Release transaction lock if it was acquired for this transaction
	if db.lockAcquiredForTransaction {
		if err := db.releaseWriteLock(db.originalLockType); err != nil {
			debugPrint("Failed to release transaction lock: %v\n", err)
		}
		db.lockAcquiredForTransaction = false
	}

	db.seqMutex.Lock()
	db.inTransaction = false
	db.seqMutex.Unlock()
}

// ------------------------------------------------------------------------------------------------
// Page Cache
// ------------------------------------------------------------------------------------------------

// addToCache adds a page to the cache
func (db *DB) addToCache(page *Page, onlyIfNotExist ...bool) {
	if page == nil {
		return
	}

	pageNumber := page.pageNumber
	bucket := &db.pageCache[pageNumber & 1023]

	bucket.mutex.Lock()
	defer bucket.mutex.Unlock()

	// If there is already a page with the same page number
	existingPage, exists := bucket.pages[pageNumber]
	if exists {
		// If we should only add if not already on cache, return early
		if len(onlyIfNotExist) > 0 && onlyIfNotExist[0] {
			return
		}
		// Avoid linking the page to itself
		if page == existingPage {
			return
		}
		// Link the new page to the existing page
		page.next = existingPage
	} else {
		// Clear the next pointer
		page.next = nil
	}

	// Add the new page to the cache
	bucket.pages[pageNumber] = page

	// Increment the total pages counter
	db.totalCachePages.Add(1)
}

// Get a page from the cache
func (db *DB) getFromCache(pageNumber uint32) (*Page, bool) {
	bucket := &db.pageCache[pageNumber & 1023]

	bucket.mutex.RLock()
	page, exists := bucket.pages[pageNumber]
	bucket.mutex.RUnlock()

	return page, exists
}

// ------------------------------------------------------------------------------------------------
// Value Cache Functions
// ------------------------------------------------------------------------------------------------

// addToValueCache adds a value to the value cache
func (db *DB) addToValueCache(offset int64, key []byte, value []byte) {
	// If the value cache is disabled, do not add to the cache
	if db.valueCacheThreshold == 0 {
		return
	}

	// Get the bucket for the offset
	bucket := &db.valueCache[offset & 255]

	bucket.mutex.Lock()
	defer bucket.mutex.Unlock()

	// Increment the access counter
	db.valueCacheAccessCounter.Add(1)
	accessTime := db.valueCacheAccessCounter.Load()

	// Check if entry already exists
	if existingEntry, exists := bucket.values[offset]; exists {
		// Update access time for existing entry
		existingEntry.accessTime = accessTime
		return
	}

	// Create new entry
	entry := &valueCacheEntry{
		accessTime: accessTime,
	}
	if key != nil {
		// Create a copy of the key
		entry.key = make([]byte, len(key))
		copy(entry.key, key)
	}
	if value != nil {
		// Create a copy of the value
		entry.value = make([]byte, len(value))
		copy(entry.value, value)
	}
	bucket.values[offset] = entry

	// Update counters
	db.totalCacheValues.Add(1)
	db.totalCacheMemory.Add(int64(len(key) + len(value)))
}

// getFromValueCache retrieves a value from the value cache
func (db *DB) getFromValueCache(offset int64, key []byte) ([]byte, bool) {
	// If the value cache is disabled, do not get from the cache
	if db.valueCacheThreshold == 0 {
		return nil, false
	}

	// Get the bucket for the offset
	bucket := &db.valueCache[offset & 255]

	bucket.mutex.Lock()
	defer bucket.mutex.Unlock()

	entry, exists := bucket.values[offset]
	if !exists {
		return nil, false
	}

	// Verify that the cached key matches the requested key
	if !equal(entry.key, key) {
		// This is a collision - the cached value is for a different key
		return nil, false
	}

	// Update access time
	db.valueCacheAccessCounter.Add(1)
	entry.accessTime = db.valueCacheAccessCounter.Load()

	// Return a copy of the value to prevent external modification
	result := make([]byte, len(entry.value))
	copy(result, entry.value)
	return result, true
}

// removeFromValueCache removes a value from the value cache
func (db *DB) removeFromValueCache(offset int64) {
	bucket := &db.valueCache[offset & 255]

	bucket.mutex.Lock()
	defer bucket.mutex.Unlock()

	if entry, exists := bucket.values[offset]; exists {
		delete(bucket.values, offset)
		db.totalCacheValues.Add(-1)
		db.totalCacheMemory.Add(-int64(len(entry.key) + len(entry.value)))
	}
}

// clearValueCache clears all entries from the value cache
func (db *DB) clearValueCache() {
	for bucketIdx := 0; bucketIdx < 256; bucketIdx++ {
		bucket := &db.valueCache[bucketIdx]
		bucket.mutex.Lock()
		bucket.values = make(map[int64]*valueCacheEntry)
		bucket.mutex.Unlock()
	}
	db.totalCacheValues.Store(0)
	db.totalCacheMemory.Store(0)
}

// cleanupOldValueCacheEntries removes approximately 50% of the oldest entries from the value cache
func (db *DB) cleanupOldValueCacheEntries() {
	totalEntries := int(db.totalCacheValues.Load())
	if totalEntries == 0 {
		return
	}

	currentAccessTime := db.valueCacheAccessCounter.Load()

	// Target to remove 50% of entries by setting cutoff time
	toRemove := uint64(totalEntries / 2)
	if toRemove == 0 {
		toRemove = 1 // Remove at least one entry worth of access time
	}

	// Calculate cutoff time to remove approximately 50% of entries
	cutoffTime := currentAccessTime - toRemove

	removedCount := int64(0)
	removedMemory := int64(0)

	for bucketIdx := 0; bucketIdx < 256; bucketIdx++ {
		bucket := &db.valueCache[bucketIdx]
		bucket.mutex.Lock()

		// Remove entries older than cutoff time
		for offset, entry := range bucket.values {
			if entry.accessTime < cutoffTime {
				delete(bucket.values, offset)
				removedCount++
				removedMemory += int64(len(entry.key) + len(entry.value))
			}
		}

		bucket.mutex.Unlock()
	}

	// Update the counters
	if removedCount > 0 {
		db.totalCacheValues.Add(-removedCount)
		db.totalCacheMemory.Add(-removedMemory)
		debugPrint("Cleaned up %d value cache entries, freed %d bytes\n", removedCount, removedMemory)
	}
}

// invalidateValueCacheFromOffset removes all cached entries with offsets >= fromOffset
// This is used during rollbacks when the file is truncated
func (db *DB) invalidateValueCacheFromOffset(fromOffset int64) {
	removedCount := int64(0)
	removedMemory := int64(0)

	for bucketIdx := 0; bucketIdx < 256; bucketIdx++ {
		bucket := &db.valueCache[bucketIdx]
		bucket.mutex.Lock()

		// Remove entries with offsets >= fromOffset
		for offset, entry := range bucket.values {
			if offset >= fromOffset {
				delete(bucket.values, offset)
				removedCount++
				removedMemory += int64(len(entry.key) + len(entry.value))
			}
		}

		bucket.mutex.Unlock()
	}

	// Update the counters
	if removedCount > 0 {
		db.totalCacheValues.Add(-removedCount)
		db.totalCacheMemory.Add(-removedMemory)
		debugPrint("Invalidated %d cached values after offset %d, freed %d bytes\n", removedCount, fromOffset, removedMemory)
	}
}

// ------------------------------------------------------------------------------------------------

// getPageAndCall gets a page from the cache and calls a callback/lambda function with the page while the lock is held
func (db *DB) getPageAndCall(pageNumber uint32, callback func(*cacheBucket, uint32, *Page)) {
	bucket := &db.pageCache[pageNumber & 1023]
	bucket.mutex.Lock()
	page, exists := bucket.pages[pageNumber]
	if exists {
		callback(bucket, pageNumber, page)
	}
	bucket.mutex.Unlock()
}

// iteratePages iterates through all pages in the cache and calls a callback/lambda function with the page while the lock is held
func (db *DB) iteratePages(callback func(*cacheBucket, uint32, *Page)) {
	for bucketIdx := 0; bucketIdx < 1024; bucketIdx++ {
		bucket := &db.pageCache[bucketIdx]
		bucket.mutex.Lock()
		// Iterate through all pages in this bucket
		for pageNumber, page := range bucket.pages {
			callback(bucket, pageNumber, page)
		}
		bucket.mutex.Unlock()
	}
}

// getWritablePage gets a writable version of a page
// if the given page is already writable, it returns the page itself
func (db *DB) getWritablePage(page *Page) (*Page, error) {
	// We cannot write to a page that is part of the WAL
	needsClone := page.isWAL
	// If the page is below the cloning mark, we need to clone it
	if page.txnSequence <= db.cloningSequence {
		needsClone = true
	}

	// If the page needs to be cloned, clone it
	if needsClone {
		var err error
		page, err = db.clonePage(page)
		if err != nil {
			return nil, fmt.Errorf("failed to clone page: %w", err)
		}
	}

	// Return the page
	return page, nil
}

// clonePage clones a page
func (db *DB) clonePage(page *Page) (*Page, error) {
	var err error
	var newPage *Page

	debugPrint("Cloning page %d\n", page.pageNumber)

	// Clone based on page type
	if page.pageType == ContentTypeTable {
		newPage, err = db.cloneTablePage(page)
		if err != nil {
			return nil, fmt.Errorf("failed to clone table page: %w", err)
		}
	} else if page.pageType == ContentTypeHybrid {
		newPage, err = db.cloneHybridPage(page)
		if err != nil {
			return nil, fmt.Errorf("failed to clone hybrid page: %w", err)
		}
	} else if page.pageNumber == 0 {
		newPage, err = db.cloneHeaderPage(page)
		if err != nil {
			return nil, fmt.Errorf("failed to clone header page: %w", err)
		}
	} else {
		return nil, fmt.Errorf("unknown page type: %c", page.pageType)
	}

	return newPage, nil
}

// markPageDirty marks a page as dirty and increments the dirty page counter
func (db *DB) markPageDirty(page *Page) {
	if !page.dirty {
		page.dirty = true
		db.dirtyPageCount++
	}
}

// markPageClean marks a page as clean and decrements the dirty page counter
func (db *DB) markPageClean(page *Page) {
	if page.dirty {
		page.dirty = false
		db.dirtyPageCount--
	}
}

// checkCache checks if the page cache is full and initiates a clean up or flush
// step 1: if the amount of dirty pages is above the threshold, flush it
//  if the page cache is below the threshold, return
// step 2: try to remove clean pages from the cache
// step 3: if the page cache is still above the threshold, flush it
// This function should not return an error, it can log an error and continue
func (db *DB) checkCache(isWrite bool) {

	// If the amount of dirty pages is above the threshold, flush them to disk
	if isWrite && db.dirtyPageCount >= db.dirtyPageThreshold {
		shouldFlush := true

		db.seqMutex.Lock()
		if db.fastRollback {
			// If already flushed up to the previous transaction, skip
			if db.inTransaction && db.flushSequence == db.txnSequence - 1 {
				shouldFlush = false
			}
		} else {
			// If already flushed up to the cloning mark, skip
			if db.inTransaction && db.flushSequence == db.cloningSequence {
				shouldFlush = false
			}
		}
		db.seqMutex.Unlock()

		if shouldFlush {
			// When the commit mode is caller thread it flushes on every commit
			// When it is worker thread, it flushes here
			if db.commitMode == WorkerThread {
				// Signal the worker thread to flush the pages, if not already signaled
				db.seqMutex.Lock()
				if !db.pendingCommands["flush"] {
					db.pendingCommands["flush"] = true
					db.workerChannel <- "flush"
				}
				db.seqMutex.Unlock()
				return
			}
		}
	}

	// If the size of the page cache is above the threshold, remove old pages
	if db.totalCachePages.Load() >= int64(db.cacheSizeThreshold) {
		// Check if we already pruned during the current transaction
		// When just reading, the inTransaction flag is false, so we can prune
		if db.inTransaction && db.pruningSequence == db.txnSequence {
			return
		}
		// Set the pruning sequence to the current transaction sequence
		db.seqMutex.Lock()
		db.pruningSequence = db.txnSequence
		db.seqMutex.Unlock()

		// Check which thread should remove the old pages
		if db.commitMode == CallerThread {
			// Discard previous versions of pages
			numPages := db.discardOldPageVersions(true)
			// If the number of pages is still greater than the cache size threshold
			if numPages > db.cacheSizeThreshold {
				// Remove old pages from cache
				db.removeOldPagesFromCache()
			}
		} else {
			// Signal the worker thread to remove the old pages, if not already signaled
			db.seqMutex.Lock()
			if !db.pendingCommands["clean"] {
				db.pendingCommands["clean"] = true
				db.workerChannel <- "clean"
			}
			db.seqMutex.Unlock()
		}
	}

	// Check the value cache
	totalMemory := db.totalCacheMemory.Load()
	if totalMemory > db.valueCacheThreshold && db.valueCacheThreshold > 0 {
		// Check which thread should clean the value cache
		if db.commitMode == CallerThread {
			// Clean the value cache on the caller thread
			db.cleanupOldValueCacheEntries()
		} else {
			// Signal the worker thread to clean the value cache, if not already signaled
			db.seqMutex.Lock()
			if !db.pendingCommands["clean_values"] {
				db.pendingCommands["clean_values"] = true
				db.workerChannel <- "clean_values"
			}
			db.seqMutex.Unlock()
		}
	}

}

// discardNewerPages removes pages from the current transaction from the cache
// This function is called by the main thread when a transaction is rolled back
func (db *DB) discardNewerPages(currentSeq int64) {
	debugPrint("Discarding pages from transaction %d and above\n", currentSeq)
	// Iterate through all pages in the cache
	db.iteratePages(func(bucket *cacheBucket, pageNumber uint32, page *Page) {
		// Skip pages from the current transaction
		// Find the first page that's not from the current transaction
		var newHead *Page = page
		var removedCount int64 = 0

		for newHead != nil && newHead.txnSequence >= currentSeq {
			debugPrint("Discarding page %d from transaction %d\n", newHead.pageNumber, newHead.txnSequence)
			// Only decrement the dirty page counter if the current page is dirty
			// and the next one isn't (to avoid incorrect counter decrements)
			if newHead.dirty && (newHead.next == nil || !newHead.next.dirty) {
				db.dirtyPageCount--
			}
			// Move to the next page
			newHead = newHead.next
			removedCount++
		}

		// Update the cache with the new head (or delete if no valid entries remain)
		if newHead != nil {
			debugPrint("Keeping page %d from transaction %d\n", newHead.pageNumber, newHead.txnSequence)
			bucket.pages[pageNumber] = newHead
		} else {
			debugPrint("No pages left for page %d\n", pageNumber)
			delete(bucket.pages, pageNumber)
		}
		// Decrement the total pages counter by the number of versions removed
		if removedCount > 0 {
			db.totalCachePages.Add(-removedCount)
		}
	})
}

// discardOldPageVersions removes older versions of pages from the cache
// keepWAL: if true, keep the first WAL page, otherwise clear the isWAL flag
// returns the number of pages kept
func (db *DB) discardOldPageVersions(keepWAL bool) int {

	db.seqMutex.Lock()
	var limitSequence int64
	if db.fastRollback {
		// If the main thread is in a transaction
		if db.inTransaction {
			// Keep pages from the previous transaction (because the current one can be rolled back)
			limitSequence = db.txnSequence - 1
		} else {
			// Otherwise, keep pages from the last committed transaction
			limitSequence = db.txnSequence
		}
	} else {
		// Keep pages below the cloning mark (because all pages after this mark can be rolled back)
		limitSequence = db.cloningSequence + 1
	}
	db.seqMutex.Unlock()

	var totalPages int64 = 0

	db.iteratePages(func(bucket *cacheBucket, pageNumber uint32, page *Page) {
		// Count and process the chain
		current := page
		var lastKept *Page = nil
		foundFirstPage := false

		for current != nil {
			totalPages++

			// Skip pages above the limit sequence - they should not be touched
			if current.txnSequence >= limitSequence {
				// If we are not keeping WAL pages, clear the isWAL flag
				if !keepWAL && current.isWAL {
					current.isWAL = false
				}
				lastKept = current
				current = current.next
				continue
			}

			shouldKeep := false
			shouldStop := false

			if keepWAL && current.isWAL {
				// Keep only the very first WAL page
				shouldKeep = true
				// After first WAL page, discard everything else
				shouldStop = true
			} else {
				if !foundFirstPage {
					// Keep only the first page before WAL pages
					foundFirstPage = true
					shouldKeep = true
				}
				if !keepWAL && current.isWAL {
					// Clear the isWAL flag
					current.isWAL = false
				}
				// Stop if we are not keeping WAL pages
				if keepWAL {
					shouldStop = false
				} else if foundFirstPage {
					shouldStop = true
				}
			}

			if shouldKeep {
				// Keep this page
				lastKept = current
			} else {
				// Discard this page
				lastKept.next = current.next
				totalPages--
			}

			if shouldStop {
				// Discard everything after this page
				lastKept.next = nil
				// Stop processing
				break
			}

			current = current.next
		}
	})

	// Update the atomic counter with the accurate count from this function
	db.totalCachePages.Store(totalPages)

	return int(totalPages)
}

// removeOldPagesFromCache removes old clean pages from the cache
// it cannot remove pages that are part of the WAL
// as other threads can be accessing these pages, this thread can only remove pages that have not been accessed recently
func (db *DB) removeOldPagesFromCache() {
	// Define a struct to hold page information for sorting
	type pageInfo struct {
		pageNumber uint32
		accessTime uint64
	}

	// Get total pages in cache from atomic counter
	totalPages := int(db.totalCachePages.Load())

	// If cache is empty or too small, nothing to do
	if totalPages <= db.cacheSizeThreshold/2 {
		return
	}

	// Compute the target size (aim to reduce to 75% of threshold)
	targetSize := db.cacheSizeThreshold * 3 / 4

	// Compute the number of pages to remove
	numPagesToRemove := totalPages - targetSize

	// Step 1: Use read locks to collect candidates
	var candidates []pageInfo

	db.seqMutex.Lock()
	var limitSequence int64
	if db.fastRollback {
		// If the main thread is in a transaction
		if db.inTransaction {
			// Keep pages from the previous transaction (because the current one can be rolled back)
			limitSequence = db.txnSequence - 1
		} else {
			// Otherwise, keep pages from the last committed transaction
			limitSequence = db.txnSequence
		}
	} else {
		// Keep pages below the cloning mark (because all pages after this mark can be rolled back)
		limitSequence = db.cloningSequence + 1
	}
	lastAccessTime := db.accessCounter
	db.seqMutex.Unlock()

	// Collect removable pages from each bucket
	db.iteratePages(func(bucket *cacheBucket, pageNumber uint32, page *Page) {
		// Skip dirty pages, WAL pages, and pages above the limit sequence
		if page.dirty || page.isWAL || page.txnSequence >= limitSequence {
			return
		}

		// Add to candidates
		candidates = append(candidates, pageInfo{
			pageNumber: pageNumber,
			accessTime: page.accessTime,
		})
	})

	// If no candidates, nothing to do
	if len(candidates) == 0 {
		return
	}

	// Step 2: Sort candidates by access time (oldest first)
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].accessTime < candidates[j].accessTime
	})

	// Step 3: Remove pages one by one with appropriate locking
	removedCount := 0
	for i := 0; i < len(candidates) && removedCount < numPagesToRemove; i++ {
		pageNumber := candidates[i].pageNumber
		bucket := &db.pageCache[pageNumber & 1023]

		bucket.mutex.Lock()

		// Double-check the page still exists and is still removable
		if page, exists := bucket.pages[pageNumber]; exists {
			// Skip if the page is dirty, WAL, or from the current transaction
			if page.dirty || page.isWAL || page.txnSequence >= limitSequence {
				bucket.mutex.Unlock()
				continue
			}

			// Skip if the page is part of a linked list with WAL pages
			hasNonRemovable := false
			for p := page.next; p != nil; p = p.next {
				if p.isWAL {
					hasNonRemovable = true
					break
				}
			}
			if hasNonRemovable {
				bucket.mutex.Unlock()
				continue
			}

			// If the page was not accessed after this function was called
			if page.accessTime < lastAccessTime {
				// Count how many page versions we're removing
				for p := page; p != nil; p = p.next {
					removedCount++
				}
				// Remove the page from the cache
				delete(bucket.pages, pageNumber)
			}
		}

		bucket.mutex.Unlock()
	}

	// Update the total pages counter
	db.totalCachePages.Add(-int64(removedCount))

	debugPrint("Removed %d pages from cache, new size: %d\n", removedCount, db.totalCachePages.Load())
}

// ------------------------------------------------------------------------------------------------
// Page access
// ------------------------------------------------------------------------------------------------

// getPage gets a page from the cache or from the disk
// If maxReadSequence > 0, only returns pages with txnSequence <= maxReadSequence
func (db *DB) getPage(pageNumber uint32, maxReadSeq ...int64) (*Page, error) {

	// Determine the maximum transaction sequence number that can be read
	var maxReadSequence int64
	if len(maxReadSeq) > 0 && maxReadSeq[0] > 0 {
		maxReadSequence = maxReadSeq[0]
	} else {
		// Default behavior: no filtering
		maxReadSequence = 0
	}

	// Get the page from the cache
	bucket := &db.pageCache[pageNumber & 1023]
	bucket.mutex.RLock()
	page, exists := bucket.pages[pageNumber]

	// Store the parent page to update the access time
	parentPage := page

	// If maxReadSequence is specified, find the latest version that's <= maxReadSequence
	if exists && maxReadSequence > 0 {
		for ; page != nil; page = page.next {
			if page.txnSequence <= maxReadSequence {
				break
			}
		}
		// If we did not find a valid page version
		if page == nil {
			exists = false
		}
	}

	// If the page is in cache, update the access time on the parent page
	if exists {
		db.accessCounter++
		parentPage.accessTime = db.accessCounter
	}

	// The mutex is still locked to avoid race conditions when updating the access time
	// Example: main thread gets page from cache, the worker thread checks the access time
	// and removes the page from the cache. the main thread will then update the access time
	// but the page will no longer be in the cache.
	bucket.mutex.RUnlock()

	// If not in cache or no valid version found
	if !exists {
		// Read the page from disk
		var err error
		page, err = db.readPage(pageNumber)
		if err != nil {
			return nil, err
		}

		// Add the page to cache
		if maxReadSequence == 0 {
			// Writer thread - always add to cache
			db.addToCache(page)
		} else {
			// Reader thread - only add if no previous version of the page is in cache
			db.addToCache(page, true)
		}
	}

	// Return the page
	return page, nil
}

// getTablePage gets a table page from the cache or from the disk
func (db *DB) getTablePage(pageNumber uint32, maxReadSequence ...int64) (*TablePage, error) {
	page, err := db.getPage(pageNumber, maxReadSequence...)
	if err != nil {
		return nil, err
	}

	// Verify it's a table page
	if page.pageType != ContentTypeTable {
		return nil, fmt.Errorf("page %d is not a table page", pageNumber)
	}

	// Return the table page
	return page, nil
}

// getHybridPage returns a hybrid page from the cache or from the disk
func (db *DB) getHybridPage(pageNumber uint32, maxReadSequence ...int64) (*HybridPage, error) {
	// Get the page from the cache or from the disk
	page, err := db.getPage(pageNumber, maxReadSequence...)
	if err != nil {
		return nil, err
	}

	// If the page is not a hybrid page, return an error
	if page.pageType != ContentTypeHybrid {
		return nil, fmt.Errorf("page %d is not a hybrid page", pageNumber)
	}

	// Return the hybrid page
	return page, nil
}

// GetCacheStats returns statistics about the page cache
func (db *DB) GetCacheStats() map[string]interface{} {
	stats := make(map[string]interface{})

	// Use the atomic counter for total pages
	totalPages := db.totalCachePages.Load()

	// Count pages by type
	tablePages := 0
	hybridPages := 0

	for bucketIdx := 0; bucketIdx < 1024; bucketIdx++ {
		bucket := &db.pageCache[bucketIdx]
		bucket.mutex.RLock()

		// Count pages by type
		for _, page := range bucket.pages {
			if page.pageType == ContentTypeTable {
				tablePages++
			} else if page.pageType == ContentTypeHybrid {
				hybridPages++
			}
		}

		bucket.mutex.RUnlock()
	}

	stats["cache_size"] = totalPages
	stats["dirty_pages"] = db.dirtyPageCount
	stats["table_pages"] = tablePages
	stats["hybrid_pages"] = hybridPages

	return stats
}

// flushIndexToDisk flushes all dirty pages to disk and writes the index header
func (db *DB) flushIndexToDisk() error {
	// Check if file is opened in read-only mode
	if db.readOnly {
		return fmt.Errorf("cannot flush index to disk: database opened in read-only mode")
	}

	// Set flush sequence number limit and determine the appropriate main file size for this flush
	db.seqMutex.Lock()
	// If a transaction is in progress
	if db.inTransaction {
		// When fast rollback is enabled
		if db.fastRollback {
			// Flush pages from the previous transaction
			db.flushSequence = db.txnSequence - 1
			// For worker thread flushes during transactions, use the file size from before the current transaction
			// This ensures we only index content that has been committed
			db.flushFileSize = db.prevFileSize
		// When fast rollback is disabled
		} else {
			// Flush at the cloning sequence number
			db.flushSequence = db.cloningSequence
			// Use the file size from when the cloning mark was set
			db.flushFileSize = db.cloningFileSize
		}
	// When no transaction is in progress
	} else {
		// Flush at the current transaction sequence number
		db.flushSequence = db.txnSequence
		// Use the current main file size
		db.flushFileSize = db.mainFileSize
	}
	db.seqMutex.Unlock()

	debugPrint("Flushing index to disk. Flush sequence: %d, Transaction sequence: %d\n", db.flushSequence, db.txnSequence)

	// Flush all dirty pages
	pagesWritten, err := db.flushDirtyIndexPages()
	if err != nil {
		return fmt.Errorf("failed to flush dirty pages: %w", err)
	}

	// If pages were written, write the index header and commit the transaction
	if pagesWritten > 0 {
		// Write index header
		if err := db.writeIndexHeader(false); err != nil {
			return fmt.Errorf("failed to update index header: %w", err)
		}
		// Commit the transaction if using WAL
		if db.useWAL {
			if err := db.walCommit(); err != nil {
				return fmt.Errorf("failed to commit WAL: %w", err)
			}
		}
	}

	// Write external values to disk
	if err := db.writeExternalValues(); err != nil {
		return fmt.Errorf("failed to write external values: %w", err)
	}

	// Update last flush time on successful flush
	db.lastFlushTime = time.Now()

	return nil
}

// flushDirtyIndexPages writes all dirty pages to disk
// Returns the number of dirty pages that were written to disk
func (db *DB) flushDirtyIndexPages() (int, error) {

	if db.flushSequence == 0 {
		return 0, fmt.Errorf("flush sequence is not set")
	}

	// Collect all page numbers
	var pageNumbers []uint32

	for bucketIdx := 0; bucketIdx < 1024; bucketIdx++ {
		bucket := &db.pageCache[bucketIdx]
		bucket.mutex.RLock()

		for pageNumber := range bucket.pages {
			pageNumbers = append(pageNumbers, pageNumber)
		}

		bucket.mutex.RUnlock()
	}

	// Sort page numbers in ascending order
	sort.Slice(pageNumbers, func(i, j int) bool {
		return pageNumbers[i] < pageNumbers[j]
	})

	// Track the number of pages written
	pagesWritten := 0

	// Process pages in order
	for _, pageNumber := range pageNumbers {
		bucket := &db.pageCache[pageNumber & 1023]
		bucket.mutex.Lock()

		// Get the page from the cache (it can be modified by another thread)
		page, _ := bucket.pages[pageNumber]

		// Find the first version of the page that was modified up to the flush sequence
		for ; page != nil; page = page.next {
			if page.txnSequence <= db.flushSequence {
				break
			}
		}
		bucket.mutex.Unlock()

		// If the page contains modifications, write it to disk
		if page != nil && page.dirty {
			var err error
			if page.pageType == ContentTypeTable {
				err = db.writeTablePage(page)
			} else if page.pageType == ContentTypeHybrid {
				err = db.writeHybridPage(page)
			}

			if err != nil {
				return pagesWritten, err
			}
			pagesWritten++
		}
	}

	return pagesWritten, nil
}

// ------------------------------------------------------------------------------------------------
// Utility functions
// ------------------------------------------------------------------------------------------------

// equal compares two byte slices
func equal(a, b []byte) bool {
	return bytes.Equal(a, b)
}

// ------------------------------------------------------------------------------------------------
// Table pages
// ------------------------------------------------------------------------------------------------

// createTablePage creates a new empty table page without allocating a page number
func (db *DB) createTablePage(pageNumber uint32) (*TablePage, error) {
	// Allocate the page data
	data := make([]byte, PageSize)

	tablePage := &TablePage{
		pageNumber:  pageNumber,
		pageType:    ContentTypeTable,
		data:        data,
		dirty:       false,
	}

	// Update the access time
	db.accessCounter++
	tablePage.accessTime = db.accessCounter

	// Update the transaction sequence
	tablePage.txnSequence = db.txnSequence

	// Add to cache
	db.addToCache(tablePage)

	debugPrint("Created new table page at page %d\n", pageNumber)

	return tablePage, nil
}

// allocateTablePage creates a new empty table page and allocates a page number
func (db *DB) allocateTablePage() (*TablePage, error) {
	// Calculate new page number
	pageNumber := uint32(db.indexFileSize / PageSize)

	// Update file size
	db.indexFileSize += PageSize

	// Create the table page
	tablePage, err := db.createTablePage(pageNumber)
	if err != nil {
		return nil, err
	}

	debugPrint("Allocated new table page at page %d\n", pageNumber)

	return tablePage, nil
}

// allocateHybridPage creates a new empty hybrid page and allocates a page number
func (db *DB) allocateHybridPage() (*HybridPage, error) {
	// Allocate the page data
	data := make([]byte, PageSize)

	// Calculate new page number
	pageNumber := uint32(db.indexFileSize / PageSize)

	// Update file size
	db.indexFileSize += PageSize

	hybridPage := &HybridPage{
		pageNumber:  pageNumber,
		pageType:    ContentTypeHybrid,
		data:        data,
		dirty:       false,
		ContentSize: HybridHeaderSize,
		SubPages:    make([]HybridSubPageInfo, 128),
	}

	// Update the access time
	db.accessCounter++
	hybridPage.accessTime = db.accessCounter

	// Update the transaction sequence
	hybridPage.txnSequence = db.txnSequence

	// Add to cache
	db.addToCache(hybridPage)

	debugPrint("Allocated new hybrid page at page %d\n", pageNumber)

	return hybridPage, nil
}

// allocateHybridPageWithSpace returns a hybrid sub-page with available space, either from the free list or creates a new one
func (db *DB) allocateHybridPageWithSpace(spaceNeeded int) (*HybridSubPage, error) {
	debugPrint("Allocating hybrid page with enough space: %d bytes\n", spaceNeeded)

	// Find a page with enough space
	pageNumber, freeSpace, position := db.findHybridPageWithSpace(spaceNeeded)

	if pageNumber > 0 {
		debugPrint("Found page %d with %d bytes free space\n", pageNumber, freeSpace)

		// Get the page
		hybridPage, err := db.getHybridPage(pageNumber)
		if err != nil {
			debugPrint("Failed to get hybrid page %d: %v\n", pageNumber, err)
			// Page not found, remove from array and try again
			db.removeFromFreeSpaceArray(position, pageNumber)
			return db.allocateHybridPageWithSpace(spaceNeeded)
		}

		// Get a writable version of the page
		hybridPage, err = db.getWritablePage(hybridPage)
		if err != nil {
			return nil, fmt.Errorf("failed to get writable page: %w", err)
		}

		// Get the current free space directly from the page
		freeSpace = PageSize - hybridPage.ContentSize
		if freeSpace < spaceNeeded {
			debugPrint("Page %d has %d bytes free space, but %d bytes are needed\n", hybridPage.pageNumber, freeSpace, spaceNeeded)
			// Remove the page from the free list
			db.updateFreeSpaceArray(position, hybridPage.pageNumber, freeSpace)
			return db.allocateHybridPageWithSpace(spaceNeeded)
		}

		// Find the first available sub-page ID
		var subPageID uint8 = 0
		// Find first available slot
		for subPageID < 128 && hybridPage.SubPages[subPageID].Offset != 0 {
			subPageID++
		}
		if subPageID == 128 {
			debugPrint("No available sub-page IDs, removing page %d from free array\n", hybridPage.pageNumber)
			// No available sub-page IDs, remove this page from the free array and try again
			db.removeFromFreeSpaceArray(position, hybridPage.pageNumber)
			return db.allocateHybridPageWithSpace(spaceNeeded)
		}

		// Count how many sub-page slots are currently used (continue from the last found slot)
		usedSlots := int(subPageID) + 1
		for i := usedSlots; i < 128; i++ {
			if hybridPage.SubPages[i].Offset != 0 {
				usedSlots++
			}
		}

		// Calculate the free space after adding the content
		freeSpaceAfter := freeSpace - spaceNeeded

		// if the free space is less than MIN_FREE_SPACE or there are no more available slots
		if freeSpaceAfter < MIN_FREE_SPACE || usedSlots == 128 {
			// Remove the page from the free list
			db.removeFromFreeSpaceArray(position, hybridPage.pageNumber)
		} else {
			// Update the free space array with the new free space amount
			// This will automatically remove the page if free space is too low
			db.updateFreeSpaceArray(position, hybridPage.pageNumber, freeSpaceAfter)
		}

		return &HybridSubPage{
			Page:       hybridPage,
			SubPageId:  subPageID,
		}, nil
	}

	// No suitable page found in the free list, allocate a new one
	newHybridPage, err := db.allocateHybridPage()
	if err != nil {
		return nil, fmt.Errorf("failed to allocate new hybrid page: %w", err)
	}
	debugPrint("Allocated new hybrid page %d\n", newHybridPage.pageNumber)

	// Add the new page to the free list since it will have free space after allocation
	freeSpaceAfter := PageSize - newHybridPage.ContentSize - spaceNeeded
	db.addToFreeHybridPagesList(newHybridPage, freeSpaceAfter)

	// Use sub-page ID 0 for the first sub-page in a new hybrid page
	return &HybridSubPage{
		Page:       newHybridPage,
		SubPageId:  0,
	}, nil
}

// cloneTablePage clones a table page
func (db *DB) cloneTablePage(page *TablePage) (*TablePage, error) {
	// Create a new page
	newPage := &TablePage{
		pageNumber:   page.pageNumber,
		pageType:     page.pageType,
		data:         make([]byte, PageSize),
		dirty:        page.dirty,
		isWAL:        false,
		accessTime:   page.accessTime,
		Salt:         page.Salt,
	}

	// Copy the data
	copy(newPage.data, page.data)

	// Update the transaction sequence
	newPage.txnSequence = db.txnSequence

	// Add to cache
	db.addToCache(newPage)

	return newPage, nil
}

// cloneHybridPage clones a hybrid page
func (db *DB) cloneHybridPage(page *HybridPage) (*HybridPage, error) {
	// Create a new page object
	newPage := &HybridPage{
		pageNumber:   page.pageNumber,
		pageType:     page.pageType,
		data:         make([]byte, PageSize),
		dirty:        page.dirty,
		isWAL:        false,
		accessTime:   page.accessTime,
		NumSubPages:  page.NumSubPages,
		ContentSize:  page.ContentSize,
		SubPages:     make([]HybridSubPageInfo, 128),
	}

	// Copy the data
	copy(newPage.data, page.data)

	// Copy the slice of sub-pages
	copy(newPage.SubPages, page.SubPages)

	// Update the transaction sequence
	newPage.txnSequence = db.txnSequence

	// Add to cache
	db.addToCache(newPage)

	return newPage, nil
}

// cloneHeaderPage clones the header page
func (db *DB) cloneHeaderPage(page *Page) (*Page, error) {
	// Create a new page
	newPage := &Page{
		pageNumber:   page.pageNumber,
		pageType:     page.pageType,
		data:         make([]byte, PageSize),
		dirty:        page.dirty,
		isWAL:        false,
		accessTime:   page.accessTime,
	}

	// Copy the data
	copy(newPage.data, page.data)

	// Deep copy the free hybrid space array if it exists
	if page.freeHybridSpaceArray != nil {
		newPage.freeHybridSpaceArray = make([]FreeSpaceEntry, len(page.freeHybridSpaceArray))
		copy(newPage.freeHybridSpaceArray, page.freeHybridSpaceArray)
	}

	// Update the transaction sequence
	newPage.txnSequence = db.txnSequence

	// Add to cache
	db.addToCache(newPage)

	return newPage, nil
}

// getHybridSubPage returns a HybridSubPage struct for the given page number and sub-page index
func (db *DB) getHybridSubPage(pageNumber uint32, SubPageId uint8, maxReadSeq ...int64) (*HybridSubPage, error) {
	// Get the page from the cache
	hybridPage, err := db.getHybridPage(pageNumber, maxReadSeq...)
	if err != nil {
		return nil, fmt.Errorf("failed to get hybrid page: %w", err)
	}

	// Check if the sub-page exists
	if int(SubPageId) >= len(hybridPage.SubPages) || hybridPage.SubPages[SubPageId].Offset == 0 {
		return nil, fmt.Errorf("sub-page with index %d not found", SubPageId)
	}

	return &HybridSubPage{
		Page:       hybridPage,
		SubPageId:  SubPageId,
	}, nil
}

// ------------------------------------------------------------------------------------------------
// Hybrid sub-page entries
// ------------------------------------------------------------------------------------------------

// HybridEntry represents a key-dataOffset pair for hybrid sub-pages
type HybridEntry struct {
	Key        []byte
	DataOffset int64
}

// addEntryToNewHybridSubPage creates a new hybrid sub-page with a single entry (convenience function)
func (db *DB) addEntryToNewHybridSubPage(parentSalt uint8, key []byte, dataOffset int64) (*HybridSubPage, error) {
	entries := []HybridEntry{{Key: key, DataOffset: dataOffset}}
	return db.addEntriesToNewHybridSubPage(parentSalt, entries)
}

// addEntriesToNewHybridSubPage creates a new hybrid sub-page, adds entries to it,
// then searches for a hybrid page with enough space to insert the sub-page into
func (db *DB) addEntriesToNewHybridSubPage(parentSalt uint8, entries []HybridEntry) (*HybridSubPage, error) {

	if len(entries) == 0 {
		return nil, fmt.Errorf("at least one entry is required")
	}

	// Generate a salt for this sub-page that avoids collisions between entries
	salt, err := db.findNonCollidingSalt(parentSalt, entries)
	if err != nil {
		return nil, fmt.Errorf("failed to find non-colliding salt: %w", err)
	}

	// Step 1: Compute the space requirements for the new sub-page
	// Entry format: slot(varint) + data_offset(8) = 9-10 bytes per entry
	subPageSize := 0  // Size of the data (excluding the header)
	for _, entry := range entries {
		slotSize := varint.Size(uint64(db.getTableSlot(entry.Key, salt))) // Get slot size with correct salt
		entrySize := slotSize + 8 // slot + data offset
		subPageSize += entrySize
	}
	totalSubPageSize := HybridSubPageHeaderSize + int(subPageSize) // 4 bytes header + data

	// Step 2: Allocate a hybrid page with enough space
	hybridSubPage, err := db.allocateHybridPageWithSpace(totalSubPageSize)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate hybrid sub-page: %w", err)
	}

	hybridPage := hybridSubPage.Page
	subPageID := hybridSubPage.SubPageId

	// Verify there's enough space (this should always be true after allocateHybridPageWithSpace)
	if hybridPage.ContentSize + totalSubPageSize > PageSize {
		return nil, fmt.Errorf("sub-page too large to fit in a hybrid page")
	}

	// Write data directly into the hybrid page
	// Calculate the offset where the sub-page will be placed
	offset := hybridPage.ContentSize

	// Write the sub-page header directly to the hybrid page
	hybridPage.data[offset] = subPageID
	hybridPage.data[offset+1] = salt
	binary.LittleEndian.PutUint16(hybridPage.data[offset+2:offset+4], uint16(subPageSize))

	// Write all entries data directly after the header
	dataPos := offset + HybridSubPageHeaderSize

	for _, entry := range entries {
		// Calculate the slot for this entry using the sub-page's salt
		slot := db.getTableSlot(entry.Key, salt)

		debugPrint("adding entry to page %d sub-page %d slot %d: dataOffset %d\n", hybridPage.pageNumber, subPageID, slot, entry.DataOffset)

		// Write slot directly
		bytesWritten := varint.Write(hybridPage.data[dataPos:], uint64(slot))
		dataPos += bytesWritten

		// Write data offset with high bit set to indicate it's a data offset (using big-endian)
		binary.BigEndian.PutUint64(hybridPage.data[dataPos:], uint64(entry.DataOffset)|0x8000000000000000)
		dataPos += 8
	}

	// Step 5: Update the hybrid page metadata
	// Update the content size
	hybridPage.ContentSize += totalSubPageSize

	// Create the HybridSubPageInfo and add it to the hybrid page
	subPageInfo := HybridSubPageInfo{
		Salt:   salt,
		Offset: uint16(offset),
		Size:   uint16(subPageSize),
	}

	// Store the sub-page at the index corresponding to its ID
	hybridPage.SubPages[subPageID] = subPageInfo

	// Update the number of sub-pages
	hybridPage.NumSubPages++

	// Mark the page as dirty
	db.markPageDirty(hybridPage)

	// Step 6: Return the HybridSubPage reference
	return &HybridSubPage{
		Page:       hybridPage,
		SubPageId:  subPageID,
	}, nil
}

// addEntryToHybridSubPage adds an entry to a specific hybrid sub-page
func (db *DB) addEntryToHybridSubPage(subPage *HybridSubPage, slot int, dataOffset int64) error {
	var err error

	// Get a writable version of the page
	subPage.Page, err = db.getWritablePage(subPage.Page)
	if err != nil {
		return fmt.Errorf("failed to get writable page: %w", err)
	}

	hybridPage := subPage.Page
	SubPageId := subPage.SubPageId

	// Get the sub-page info
	if int(SubPageId) >= len(hybridPage.SubPages) || hybridPage.SubPages[SubPageId].Offset == 0 {
		return fmt.Errorf("sub-page with index %d not found", SubPageId)
	}
	subPageInfo := &hybridPage.SubPages[SubPageId]

	// Calculate the size needed for the new entry
	slotSize := varint.Size(uint64(slot))
	newEntrySize := slotSize + 8 // slot + data offset (with high bit set)

	// Calculate the total size needed for the updated sub-page
	newSubPageSize := int(subPageInfo.Size) + newEntrySize

	debugPrint("Adding entry to hybrid page %d sub-page %d. Current sub-page size %d new entry size %d. Total used space on page %d, available space %d\n",
		hybridPage.pageNumber, SubPageId, subPageInfo.Size, newEntrySize, hybridPage.ContentSize, PageSize - hybridPage.ContentSize)

	// Check if there's enough space in the current page for the updated sub-page
	if hybridPage.ContentSize + newEntrySize > PageSize {
		// Current hybrid page doesn't have enough space for the expanded sub-page
		// Check if the sub-page (with new entry) can fit in a new empty hybrid page
		subPageWithHeaderSize := HybridSubPageHeaderSize + newSubPageSize
		if HybridHeaderSize + subPageWithHeaderSize <= PageSize {
			// Sub-page can fit in a new hybrid page, move it there
			err = db.moveSubPageToNewHybridPage(subPage, slot, dataOffset)
			if err != nil {
				return fmt.Errorf("failed to move sub-page to new hybrid page: %w", err)
			}
		} else {
			// Sub-page is too large even for a new hybrid page, convert to table page
			err = db.convertHybridSubPageToTablePage(subPage, slot, dataOffset)
			if err != nil {
				return fmt.Errorf("failed to convert hybrid sub-page to table page: %w", err)
			}
		}
		return nil
	}

	// There's enough space in the current page, proceed with the update
	// Step 1: Get the offset of the current sub-page (end of existing entries)
	currentSubPageStart := int(subPageInfo.Offset) + HybridSubPageHeaderSize // Skip header
	currentSubPageEnd := currentSubPageStart + int(subPageInfo.Size)

	// Step 2: Move data (all subsequent sub-pages after the current) to open space for the new entry
	if currentSubPageEnd < hybridPage.ContentSize {
		// Move all data after this sub-page to make room for the new entry
		copy(hybridPage.data[currentSubPageEnd+newEntrySize:], hybridPage.data[currentSubPageEnd:hybridPage.ContentSize])
	}

	// Step 3: Serialize the new entry directly in the opened space
	entryPos := currentSubPageEnd

	// Write slot directly
	bytesWritten := varint.Write(hybridPage.data[entryPos:], uint64(slot))
	entryPos += bytesWritten

	// Write data offset with high bit set to indicate it's a data offset (using big-endian)
	binary.BigEndian.PutUint64(hybridPage.data[entryPos:], uint64(dataOffset)|0x8000000000000000)

	// Step 4: Update metadata
	// Update the sub-page size in the header
	newSubPageSizeUint16 := uint16(newSubPageSize)
	binary.LittleEndian.PutUint16(hybridPage.data[subPageInfo.Offset+2:subPageInfo.Offset+4], newSubPageSizeUint16)

	// Update the sub-page info
	subPageInfo.Size = newSubPageSizeUint16

	// Update offsets for sub-pages that come after this one
	for i := 0; i < len(hybridPage.SubPages); i++ {
		if hybridPage.SubPages[i].Offset != 0 && hybridPage.SubPages[i].Offset > subPageInfo.Offset {
			hybridPage.SubPages[i].Offset += uint16(newEntrySize)
		}
	}

	// Update the hybrid page content size
	hybridPage.ContentSize += newEntrySize

	// Mark the page as dirty
	db.markPageDirty(hybridPage)

	// Add the hybrid page to the free list if it has reasonable free space
	db.addToFreeHybridPagesList(hybridPage, 0)

	return nil
}

// removeEntryFromHybridSubPage removes an entry from a hybrid sub-page at the given offset
func (db *DB) removeEntryFromHybridSubPage(subPage *HybridSubPage, entryOffset int, entrySize int) error {
	// Get a writable version of the page
	hybridPage, err := db.getWritablePage(subPage.Page)
	if err != nil {
		return fmt.Errorf("failed to get writable page: %w", err)
	}
	// Update the subPage reference to point to the writable page
	subPage.Page = hybridPage

	// Get the sub-page info
	SubPageId := subPage.SubPageId
	if int(SubPageId) >= len(hybridPage.SubPages) || hybridPage.SubPages[SubPageId].Offset == 0 {
		return fmt.Errorf("sub-page with index %d not found", SubPageId)
	}
	subPageInfo := &hybridPage.SubPages[SubPageId]

	// Calculate positions (entryOffset is already relative to hybrid page data)
	entryEnd := entryOffset + entrySize

	// Single copy: move all data after the removed entry to fill the gap
	// This includes both data within the sub-page and all data after the sub-page
	if entryEnd < hybridPage.ContentSize {
		copy(hybridPage.data[entryOffset:], hybridPage.data[entryEnd:hybridPage.ContentSize])
	}

	// Update the sub-page size
	newSubPageSize := int(subPageInfo.Size) - entrySize
	subPageInfo.Size = uint16(newSubPageSize)

	// Update the sub-page size in the header
	binary.LittleEndian.PutUint16(hybridPage.data[subPageInfo.Offset+2:subPageInfo.Offset+4], uint16(newSubPageSize))

	// Update offsets for sub-pages that come after this one
	for i := 0; i < len(hybridPage.SubPages); i++ {
		if hybridPage.SubPages[i].Offset != 0 && hybridPage.SubPages[i].Offset > uint16(entryOffset) {
			hybridPage.SubPages[i].Offset -= uint16(entrySize)
		}
	}

	// Update the hybrid page content size
	hybridPage.ContentSize -= entrySize

	// Mark the page as dirty
	db.markPageDirty(hybridPage)

	// Add the hybrid page to the free list if it has reasonable free space
	db.addToFreeHybridPagesList(hybridPage, 0)

	return nil
}

// updateDataOffsetInHybridSubPage updates the data offset of an entry in a hybrid sub-page
func (db *DB) updateDataOffsetInHybridSubPage(subPage *HybridSubPage, entryOffset int, entrySize int, dataOffset int64) error {
	var err error

	// Get a writable version of the page
	subPage.Page, err = db.getWritablePage(subPage.Page)
	if err != nil {
		return fmt.Errorf("failed to get writable page: %w", err)
	}
	hybridPage := subPage.Page

	// Calculate the position where the data offset is stored (last 8 bytes of the entry)
	dataOffsetPosition := entryOffset + entrySize - 8

	// Update the data offset in-place with high bit set (using big-endian)
	binary.BigEndian.PutUint64(hybridPage.data[dataOffsetPosition:], uint64(dataOffset)|0x8000000000000000)

	// Mark the page as dirty
	db.markPageDirty(hybridPage)

	return nil
}

// updateSubPagePointerInHybridSubPage updates the sub-page pointer of an entry in a hybrid sub-page
func (db *DB) updateSubPagePointerInHybridSubPage(subPage *HybridSubPage, entryOffset int, entrySize int, pageNumber uint32, subPageId uint8) error {
	// Check if the sub-page ID is valid
	if subPageId > 127 {
		return fmt.Errorf("sub-page ID out of range")
	}

	// Get a writable version of the page
	hybridPage, err := db.getWritablePage(subPage.Page)
	if err != nil {
		return fmt.Errorf("failed to get writable page: %w", err)
	}
	// Update the subPage reference to point to the writable page
	subPage.Page = hybridPage

	// Calculate the position where the sub-page pointer is stored (last 5 bytes of the entry)
	subPagePointerPosition := entryOffset + entrySize - 5

	// Update the sub-page pointer in-place
	// First byte: sub-page ID (without high bit set, indicating it's a sub-page pointer)
	hybridPage.data[subPagePointerPosition] = subPageId
	// Next 4 bytes: page number
	binary.LittleEndian.PutUint32(hybridPage.data[subPagePointerPosition+1:subPagePointerPosition+5], pageNumber)

	// Mark the page as dirty
	db.markPageDirty(hybridPage)

	return nil
}

// convertEntryInHybridSubPage converts a data offset entry (8 bytes) to a sub-page pointer entry (5 bytes)
// This moves all subsequent data 3 bytes to the left and updates the entry in-place
func (db *DB) convertEntryInHybridSubPage(subPage *HybridSubPage, entryOffset int, entrySize int, pageNumber uint32, subPageId uint8) error {
	// Check if the sub-page ID is valid
	if subPageId > 127 {
		return fmt.Errorf("sub-page ID out of range")
	}

	// Get a writable version of the page
	hybridPage, err := db.getWritablePage(subPage.Page)
	if err != nil {
		return fmt.Errorf("failed to get writable page: %w", err)
	}
	// Update the subPage reference to point to the writable page
	subPage.Page = hybridPage

	// Get the sub-page info
	SubPageId := subPage.SubPageId
	if int(SubPageId) >= len(hybridPage.SubPages) || hybridPage.SubPages[SubPageId].Offset == 0 {
		return fmt.Errorf("sub-page with index %d not found", SubPageId)
	}
	subPageInfo := &hybridPage.SubPages[SubPageId]

	// Calculate positions
	valueStartPos := entryOffset + entrySize - 8  // Start of the 8-byte data offset
	valueEndPos := entryOffset + entrySize        // End of the entry

	// Write the new 5-byte sub-page pointer in place of the 8-byte data offset
	// First byte: sub-page ID (without high bit set, indicating it's a sub-page pointer)
	hybridPage.data[valueStartPos] = subPageId
	// Next 4 bytes: page number
	binary.LittleEndian.PutUint32(hybridPage.data[valueStartPos+1:valueStartPos+5], pageNumber)

	// Move all data after this entry 3 bytes to the left (since we're reducing from 8 to 5 bytes)
	moveStartPos := valueEndPos
	moveAmount := 3 // 8 bytes - 5 bytes = 3 bytes to compress

	if moveStartPos < hybridPage.ContentSize {
		copy(hybridPage.data[moveStartPos-moveAmount:], hybridPage.data[moveStartPos:hybridPage.ContentSize])
	}

	// Update the sub-page size (reduce by 3 bytes)
	newSubPageSize := int(subPageInfo.Size) - moveAmount
	subPageInfo.Size = uint16(newSubPageSize)

	// Update the sub-page size in the header
	binary.LittleEndian.PutUint16(hybridPage.data[subPageInfo.Offset+2:subPageInfo.Offset+4], uint16(newSubPageSize))

	// Update offsets for sub-pages that come after this one
	for i := 0; i < len(hybridPage.SubPages); i++ {
		if hybridPage.SubPages[i].Offset != 0 && hybridPage.SubPages[i].Offset > uint16(entryOffset) {
			hybridPage.SubPages[i].Offset -= uint16(moveAmount)
		}
	}

	// Update the hybrid page content size
	hybridPage.ContentSize -= moveAmount

	// Mark the page as dirty
	db.markPageDirty(hybridPage)

	// Add the hybrid page to the free list if it has reasonable free space
	db.addToFreeHybridPagesList(hybridPage, 0)

	return nil
}

// ------------------------------------------------------------------------------------------------
// Table entries
// ------------------------------------------------------------------------------------------------

// setTableEntry sets an entry in a table page
func (db *DB) setTableEntry(tablePage *TablePage, slot int, pageNumber uint32, subPageId uint8) error {
	// Check if slot is valid
	if slot < 0 || slot >= TableEntries {
		return fmt.Errorf("slot index out of range")
	}

	// Get a writable version of the page
	tablePage, err := db.getWritablePage(tablePage)
	if err != nil {
		return err
	}

	// Calculate the offset for this entry in the page data
	offset := TableHeaderSize + (slot * TableEntrySize)

	// Write page number (4 bytes)
	binary.LittleEndian.PutUint32(tablePage.data[offset:offset+4], pageNumber)

	// Write sub-page id (1 byte)
	tablePage.data[offset+4] = subPageId

	// Mark page as dirty
	db.markPageDirty(tablePage)

	return nil
}

// getTableEntry reads a table entry from the specified slot in a table page
// Returns pageNumber and subPageId
func (db *DB) getTableEntry(tablePage *TablePage, slot int) (uint32, uint8) {
	if slot < 0 || slot >= TableEntries {
		return 0, 0
	}

	// Calculate the offset for this entry in the page data
	offset := TableHeaderSize + (slot * TableEntrySize)

	// Read page number (4 bytes)
	pageNumber := binary.LittleEndian.Uint32(tablePage.data[offset:offset+4])

	// Read sub-page id (1 byte)
	subPageId := tablePage.data[offset+4]

	// Return the page number and sub-page id
	return pageNumber, subPageId
}

// ------------------------------------------------------------------------------------------------
// Preload
// ------------------------------------------------------------------------------------------------

// preloadMainHashTable preloads the main hash table into the cache
func (db *DB) preloadMainHashTable() error {
	// Load all pages in the main hash table
	for pageNumber := 1; pageNumber <= db.mainIndexPages; pageNumber++ {
		_, err := db.getTablePage(uint32(pageNumber))
		if err != nil {
			return fmt.Errorf("failed to read main index page %d: %w", pageNumber, err)
		}
	}
	return nil
}

// convertHybridSubPageToTablePage converts a hybrid sub-page to a table page when it's too large
// It creates a new table page using the same page number as the hybrid page and redistributes all entries from the hybrid sub-page
func (db *DB) convertHybridSubPageToTablePage(subPage *HybridSubPage, newSlot int, newDataOffset int64) error {
	hybridPage := subPage.Page
	SubPageId := subPage.SubPageId

	debugPrint("Converting hybrid sub-page %d on page %d to table page\n", SubPageId, hybridPage.pageNumber)

	// Check if the sub-page exists
	if int(SubPageId) >= len(hybridPage.SubPages) || hybridPage.SubPages[SubPageId].Offset == 0 {
		return fmt.Errorf("sub-page with index %d not found", SubPageId)
	}
	subPageInfo := &hybridPage.SubPages[SubPageId]

	// Remove the old hybrid page from free space array
	db.removeFromFreeSpaceArray(-1, hybridPage.pageNumber)

	// Create a new table page using the same page number as the hybrid page
	newTablePage, err := db.createTablePage(hybridPage.pageNumber)
	if err != nil {
		return fmt.Errorf("failed to create table page: %w", err)
	}

	// Set the salt for the table page (use the same salt as the sub-page)
	newTablePage.Salt = subPageInfo.Salt

	// Collect data offset entries grouped by slot
	slotEntries := make(map[int][]HybridEntry)

	// Single pass: process all existing entries
	err = db.iterateHybridSubPageEntries(hybridPage, SubPageId, func(entryOffset int, entrySize int, slot int, isSubPage bool, value uint64) bool {
		if isSubPage {
			// Copy sub-page pointers directly to the table page
			nextSubPageId := uint8(value & 0xFF)
			nextPageNumber := uint32((value >> 8) & 0xFFFFFFFF)

			setErr := db.setTableEntry(newTablePage, slot, nextPageNumber, nextSubPageId)
			if setErr != nil {
				debugPrint("Failed to set table entry for slot %d: %v\n", slot, setErr)
			}
		} else {
			// For data offsets, read content and group by slot
			dataOffset := int64(value)
			content, readErr := db.readContent(dataOffset)
			if readErr != nil {
				debugPrint("Failed to read content at offset %d: %v\n", dataOffset, readErr)
				return true // Continue iteration despite error
			}

			// Add to the slot's entries
			if slotEntries[slot] == nil {
				slotEntries[slot] = []HybridEntry{}
			}
			slotEntries[slot] = append(slotEntries[slot], HybridEntry{
				Key:        content.key,
				DataOffset: dataOffset,
			})
		}
		return true // Continue iteration
	})
	if err != nil {
		return fmt.Errorf("failed to iterate existing entries: %w", err)
	}

	// Add the new entry to the appropriate slot
	content, err := db.readContent(newDataOffset)
	if err != nil {
		return fmt.Errorf("failed to read content for new entry: %w", err)
	}
	if slotEntries[newSlot] == nil {
		slotEntries[newSlot] = []HybridEntry{}
	}
	slotEntries[newSlot] = append(slotEntries[newSlot], HybridEntry{
		Key:        content.key,
		DataOffset: newDataOffset,
	})

	// Create new hybrid sub-pages for slots with data offsets
	for slot, entries := range slotEntries {
		if len(entries) > 0 {
			// Create a new hybrid sub-page with these entries
			newHybridSubPage, err := db.addEntriesToNewHybridSubPage(subPageInfo.Salt, entries)
			if err != nil {
				return fmt.Errorf("failed to create hybrid sub-page for slot %d: %w", slot, err)
			}

			// Store the pointer to this new hybrid sub-page in the table page
			err = db.setTableEntry(newTablePage, slot, newHybridSubPage.Page.pageNumber, newHybridSubPage.SubPageId)
			if err != nil {
				return fmt.Errorf("failed to set table entry for slot %d: %w", slot, err)
			}
		}
	}

	// Remove the old sub-page from the hybrid page
	db.removeSubPageFromHybridPage(hybridPage, SubPageId)

	return nil
}

// moveSubPageToNewHybridPage moves a hybrid sub-page to a new hybrid page when it doesn't fit in the current page
// but is still small enough to fit in a new empty hybrid page
func (db *DB) moveSubPageToNewHybridPage(subPage *HybridSubPage, slot int, dataOffset int64) error {
	hybridPage := subPage.Page
	SubPageId := subPage.SubPageId

	debugPrint("Moving hybrid sub-page %d from page %d to new hybrid page\n", SubPageId, hybridPage.pageNumber)

	// Get the sub-page info
	if int(SubPageId) >= len(hybridPage.SubPages) || hybridPage.SubPages[SubPageId].Offset == 0 {
		return fmt.Errorf("sub-page with index %d not found", SubPageId)
	}
	subPageInfo := &hybridPage.SubPages[SubPageId]

	// Step 1: Compute the total new space needed
	slotSize := varint.Size(uint64(slot))
	newEntrySize := slotSize + 8 // slot + data offset
	newSubPageSize := int(subPageInfo.Size) + newEntrySize
	totalSubPageSize := HybridSubPageHeaderSize + newSubPageSize

	// Step 2: Get hybrid page with enough space
	newHybridSubPage, err := db.allocateHybridPageWithSpace(totalSubPageSize)
	if err != nil {
		return fmt.Errorf("failed to allocate new hybrid sub-page: %w", err)
	}

	newHybridPage := newHybridSubPage.Page
	newSubPageID := newHybridSubPage.SubPageId

	// Step 3: Copy sub-page directly from page A to page B (at the end)
	// Calculate the offset where the sub-page will be placed in the new hybrid page
	offset := int(newHybridPage.ContentSize)

	// Write the sub-page header directly to the new hybrid page
	newHybridPage.data[offset] = newSubPageID // Sub-page ID
	newHybridPage.data[offset+1] = subPageInfo.Salt // Salt
	binary.LittleEndian.PutUint16(newHybridPage.data[offset+2:offset+4], uint16(newSubPageSize))

	// Copy existing entries directly from the original sub-page to the new page
	originalStart := int(subPageInfo.Offset) + HybridSubPageHeaderSize // Skip header
	dataPos := offset + HybridSubPageHeaderSize // Skip new header
	copy(newHybridPage.data[dataPos:], hybridPage.data[originalStart:originalStart+int(subPageInfo.Size)])
	dataPos += int(subPageInfo.Size)

	// Step 4: Serialize the new entry directly in the new page
	// Write slot directly
	bytesWritten := varint.Write(newHybridPage.data[dataPos:], uint64(slot))
	dataPos += bytesWritten

	// Write data offset with high bit set to indicate it's a data offset (using big-endian)
	binary.BigEndian.PutUint64(newHybridPage.data[dataPos:], uint64(dataOffset)|0x8000000000000000)

	// Update the new hybrid page metadata
	newHybridPage.ContentSize += totalSubPageSize

	// Create the HybridSubPageInfo and add it to the new hybrid page
	newHybridPage.SubPages[newSubPageID] = HybridSubPageInfo{
		Salt:   subPageInfo.Salt,
		Offset: uint16(offset),
		Size:   uint16(newSubPageSize),
	}

	// Update the number of sub-pages
	newHybridPage.NumSubPages++

	// Mark the new page as dirty
	db.markPageDirty(newHybridPage)

	// Remove the sub-page from the original hybrid page
	db.removeSubPageFromHybridPage(hybridPage, SubPageId)

	// Update the subPage reference to point to the new hybrid page
	subPage.Page = newHybridPage
	subPage.SubPageId = newSubPageID

	return nil
}

// removeSubPageFromHybridPage removes a sub-page from a hybrid page
func (db *DB) removeSubPageFromHybridPage(hybridPage *HybridPage, SubPageId uint8) {
	// Get the sub-page info
	if int(SubPageId) >= len(hybridPage.SubPages) || hybridPage.SubPages[SubPageId].Offset == 0 {
		return // Sub-page doesn't exist, nothing to remove
	}
	subPageInfo := &hybridPage.SubPages[SubPageId]

	debugPrint("Removing sub-page %d from hybrid page %d\n", SubPageId, hybridPage.pageNumber)

	// Calculate the sub-page boundaries
	subPageStart := int(subPageInfo.Offset)
	subPageSize := HybridSubPageHeaderSize + int(subPageInfo.Size)
	subPageEnd := subPageStart + subPageSize

	// Move all data after this sub-page to fill the gap
	if subPageEnd < hybridPage.ContentSize {
		copy(hybridPage.data[subPageStart:], hybridPage.data[subPageEnd:hybridPage.ContentSize])
	}

	// Update offsets for sub-pages that come after this one
	for i := 0; i < len(hybridPage.SubPages); i++ {
		if hybridPage.SubPages[i].Offset != 0 && hybridPage.SubPages[i].Offset > subPageInfo.Offset {
			hybridPage.SubPages[i].Offset -= uint16(subPageSize)
		}
	}

	// Remove the sub-page from the array by clearing its offset
	hybridPage.SubPages[SubPageId] = HybridSubPageInfo{}

	// Update content size
	hybridPage.ContentSize -= subPageSize

	// Update the number of sub-pages
	hybridPage.NumSubPages--

	// Mark the page as dirty
	db.markPageDirty(hybridPage)

	// Add the hybrid page to the free list if it has reasonable free space
	db.addToFreeHybridPagesList(hybridPage, 0)
}

// ------------------------------------------------------------------------------------------------
// Free lists
// ------------------------------------------------------------------------------------------------

// addToFreeHybridPagesList adds a hybrid page with free space to the array
func (db *DB) addToFreeHybridPagesList(hybridPage *HybridPage, freeSpace int) {
	db.addToFreeSpaceArray(hybridPage, freeSpace)
}

// ------------------------------------------------------------------------------------------------
// Free space array management
// ------------------------------------------------------------------------------------------------

// addToFreeSpaceArray adds or updates a hybrid page in the free space array
func (db *DB) addToFreeSpaceArray(hybridPage *HybridPage, freeSpace int) {

	if freeSpace == 0 {
		// Calculate free space
		freeSpace = PageSize - hybridPage.ContentSize
	}

	if hybridPage.ContentSize > PageSize {
		debugPrint("PANIC_CONDITION_MET: Page %d has content size %d which is greater than PageSize %d\n", hybridPage.pageNumber, hybridPage.ContentSize, PageSize)
		// Panic to capture the stack trace
		panic(fmt.Sprintf("Page %d has content size greater than page size: %d", hybridPage.pageNumber, hybridPage.ContentSize))
	}

	// Only add if the page has reasonable free space (at least MIN_FREE_SPACE bytes)
	if freeSpace < MIN_FREE_SPACE {
		return
	}
	// Only add if the page has less than 128 sub-pages
	if hybridPage.NumSubPages >= 128 {
		return
	}

	debugPrint("Adding page %d to free hybrid space array with %d bytes of free space\n", hybridPage.pageNumber, freeSpace)

	// Get the header page
	headerPage := db.headerPageForTransaction

	// Find the entry with minimum free space by iterating through the array
	minFreeSpace := uint16(PageSize) // Start with max possible value
	minIndex := -1

	// Iterate through the array
	for i, entry := range headerPage.freeHybridSpaceArray {
		// Look for existing entry
		if entry.PageNumber == hybridPage.pageNumber {
			// If found, update existing entry
			headerPage.freeHybridSpaceArray[i].FreeSpace = uint16(freeSpace)
			return
		}
		// Find the entry with minimum free space
		if entry.FreeSpace < minFreeSpace {
			minFreeSpace = entry.FreeSpace
			minIndex = i
		}
	}

	// Add new entry
	newEntry := FreeSpaceEntry{
		PageNumber: hybridPage.pageNumber,
		FreeSpace:  uint16(freeSpace),
	}

	// If array is full, remove the entry with least free space
	if len(headerPage.freeHybridSpaceArray) >= MaxFreeSpaceEntries {
		// Only add if new entry has more free space than the minimum found
		if uint16(freeSpace) > minFreeSpace {
			// Replace the entry with the new entry
			headerPage.freeHybridSpaceArray[minIndex] = newEntry
			return
		} else {
			// Don't add this entry
			return
		}
	}

	// Add the new entry
	headerPage.freeHybridSpaceArray = append(headerPage.freeHybridSpaceArray, newEntry)
}

// removeFromFreeSpaceArray removes a hybrid page from the free space array
func (db *DB) removeFromFreeSpaceArray(position int, pageNumber uint32) {
	debugPrint("Removing page %d from free space array\n", pageNumber)

	// Get the header page
	headerPage := db.headerPageForTransaction

	// If position is -1, search for the page number in the array
	if position == -1 {
		for i, entry := range headerPage.freeHybridSpaceArray {
			if entry.PageNumber == pageNumber {
				position = i
				break
			}
		}
	}

	// Replace this entry with the last entry (to avoid memory move)
	arrayLen := len(headerPage.freeHybridSpaceArray)
	if position >= 0 && position < arrayLen {
		// Copy the last element to this position
		headerPage.freeHybridSpaceArray[position] = headerPage.freeHybridSpaceArray[arrayLen-1]
		// Shrink the array
		headerPage.freeHybridSpaceArray = headerPage.freeHybridSpaceArray[:arrayLen-1]
	}
}

// findHybridPageWithSpace finds a hybrid page with at least the specified amount of free space
// Returns the page number and the amount of free space, or 0 if no suitable page is found
func (db *DB) findHybridPageWithSpace(spaceNeeded int) (uint32, int, int) {
	debugPrint("Finding hybrid page with space: %d\n", spaceNeeded)

	// Get the header page
	headerPage := db.headerPageForTransaction

	// Optimization: iterate forward for better cache locality
	// Find the best fit (page with just enough space)
	bestFitPageNumber := uint32(0)
	bestFitSpace := PageSize + 1 // Start with a value larger than any possible free space
	bestFitPosition := -1

	// First pass: look for a page with exactly enough space or slightly more
	for position, entry := range headerPage.freeHybridSpaceArray {
		entrySpace := int(entry.FreeSpace)
		// If this is a better fit than what we've found so far
		if entrySpace >= spaceNeeded && entrySpace < bestFitSpace {
			bestFitPageNumber = entry.PageNumber
			bestFitSpace = entrySpace
			bestFitPosition = position
		}
	}

	// If we found any page with enough space, return it
	if bestFitPageNumber > 0 {
		return bestFitPageNumber, bestFitSpace, bestFitPosition
	}

	// No page found with enough space
	return 0, 0, 0
}

// updateFreeSpaceArray updates the free space for a specific page in the array
func (db *DB) updateFreeSpaceArray(position int, pageNumber uint32, newFreeSpace int) {
	debugPrint("Updating free space array for page %d to %d\n", pageNumber, newFreeSpace)

	// If free space is too low, just remove the entry
	if newFreeSpace < MIN_FREE_SPACE {
		db.removeFromFreeSpaceArray(position, pageNumber)
		return
	}

	// Get the header page
	headerPage := db.headerPageForTransaction

	// Update the entry
	headerPage.freeHybridSpaceArray[position].FreeSpace = uint16(newFreeSpace)
}

// ------------------------------------------------------------------------------------------------
// Recovery
// ------------------------------------------------------------------------------------------------

// recoverUnindexedContent reads the main file starting from the last indexed offset
// and reindexes any content that hasn't been indexed yet
// It also checks for commit markers and discards any uncommitted data
func (db *DB) recoverUnindexedContent() error {

	lastIndexedOffset := db.lastIndexedOffset

	// If the last indexed offset is 0, start from the beginning (after header)
	if lastIndexedOffset < int64(PageSize) {
		lastIndexedOffset = int64(PageSize)
	}

	// If the last indexed offset is already at the end of the file, nothing to do
	if lastIndexedOffset >= db.mainFileSize {
		return nil
	}

	debugPrint("Recovering unindexed content from offset %d to %d\n", lastIndexedOffset, db.mainFileSize)

	// First pass: Find the last valid commit marker and truncate file if needed
	validFileSize, err := db.findLastValidCommit(lastIndexedOffset)
	if err != nil {
		return fmt.Errorf("failed to find last valid commit: %w", err)
	}

	// If we need to truncate the file due to uncommitted data
	if validFileSize < db.mainFileSize {
		debugPrint("Truncating main file from %d to %d due to uncommitted data\n", db.mainFileSize, validFileSize)
		if !db.readOnly {
			if err := db.mainFile.Truncate(validFileSize); err != nil {
				return fmt.Errorf("failed to truncate main file: %w", err)
			}
		}
		db.mainFileSize = validFileSize
	}

	// If there's nothing to recover after truncation
	if lastIndexedOffset >= db.mainFileSize {
		return nil
	}

	// Initialize the transaction sequence
	err = db.beginTransaction()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Reindex the content
	err = db.reindexContent(lastIndexedOffset)

	if err != nil {
		db.rollbackTransaction()
		return fmt.Errorf("failed to reindex content: %w", err)
	}

	// Commit the transaction
	db.commitTransaction()

	if !db.readOnly {
		// Flush the index pages to disk
		if err := db.flushIndexToDisk(); err != nil {
			return fmt.Errorf("failed to flush index to disk: %w", err)
		}
	}

	debugPrint("Recovery complete, reindexed content up to offset %d\n", db.mainFileSize)
	return nil
}

func (db *DB) reindexContent(lastIndexedOffset int64) error {

	// If the last indexed offset is before the header page, set it to the header page
	if lastIndexedOffset < int64(PageSize) {
		lastIndexedOffset = int64(PageSize)
	}

	debugPrint("Reindexing content from offset %d to %d\n", lastIndexedOffset, db.mainFileSize)

	// Second pass: Process all committed data
	currentOffset := lastIndexedOffset

	for currentOffset < db.mainFileSize {
		// Read the content at the current offset
		content, err := db.readContent(currentOffset)
		if err != nil {
			return fmt.Errorf("failed to read content at offset %d: %w", currentOffset, err)
		}

		if content.data[0] == ContentTypeData {
			debugPrint("Reindexing data at offset %d - key: %s, value: %s\n", currentOffset, content.key, content.value)
			// Set the key-value pair on the index
			err := db.setKvOnIndex(content.key, content.value, currentOffset)
			if err != nil {
				return fmt.Errorf("failed to set kv on index: %w", err)
			}
		} else if content.data[0] == ContentTypeCommit {
			// Do nothing, we've already processed the commit marker
		}

		// Move to the next content
		currentOffset += int64(len(content.data))
	}

	return nil
}

// findLastValidCommit scans the file from the given offset to find the last valid commit marker
// and returns the file size that should be used (truncating any uncommitted data)
func (db *DB) findLastValidCommit(startOffset int64) (int64, error) {
	currentOffset := startOffset
	lastValidOffset := startOffset
	runningChecksum := uint32(0)

	for currentOffset < db.mainFileSize {
		// Read the content type first
		typeBuffer := make([]byte, 1)
		if _, err := db.mainFile.ReadAt(typeBuffer, currentOffset); err != nil {
			if err == io.EOF {
				break
			}
			return 0, fmt.Errorf("failed to read content type at offset %d: %w", currentOffset, err)
		}

		contentType := typeBuffer[0]

		if contentType == ContentTypeData {
			// Read the full data content to get its size and update checksum
			content, err := db.readContent(currentOffset)
			if err != nil {
				// If we can't read the content, it's likely corrupted or incomplete
				debugPrint("Failed to read data content at offset %d: %v\n", currentOffset, err)
				break
			}
			// Update running checksum with this data content
			runningChecksum = crc32.Update(runningChecksum, crc32.IEEETable, content.data)
			currentOffset += int64(len(content.data))
		} else if contentType == ContentTypeCommit {
			// Read the checksum from the commit marker
			checksum := make([]byte, 4)
			if _, err := db.mainFile.ReadAt(checksum, currentOffset + 1); err != nil {
				debugPrint("Failed to read checksum at offset %d: %v\n", currentOffset + 1, err)
				break
			}
			// Extract the stored checksum
			storedChecksum := binary.BigEndian.Uint32(checksum)
			// Verify checksum matches our running checksum
			if storedChecksum != runningChecksum {
				debugPrint("Invalid commit marker at offset %d: checksum mismatch expected %d, got %d\n", currentOffset, runningChecksum, storedChecksum)
				break
			}
			// This is a valid commit, update our last valid position
			currentOffset += 5 // Commit marker is always 5 bytes
			lastValidOffset = currentOffset
			// Reset running checksum for next transaction
			runningChecksum = 0
		} else {
			// Unknown content type, stop processing
			debugPrint("Unknown content type '%c' at offset %d\n", contentType, currentOffset)
			break
		}
	}

	return lastValidOffset, nil
}

// ------------------------------------------------------------------------------------------------
// System
// ------------------------------------------------------------------------------------------------

// calculateDefaultCacheSize calculates the default cache size threshold based on system memory
// Returns the number of pages that can fit in 20% of the system memory
func calculateDefaultCacheSize() int {
	totalMemory := getTotalSystemMemory()

	// Use 20% of total memory for cache
	cacheMemory := int64(float64(totalMemory) * 0.2)

	// Calculate how many pages fit in the cache memory
	numPages := int(cacheMemory / PageSize)

	// Ensure we have a reasonable minimum
	if numPages < 300 {
		numPages = 300
	}

	debugPrint("System memory: %d bytes, Cache memory: %d bytes, Cache pages: %d\n",
		totalMemory, cacheMemory, numPages)

	return numPages
}

// getTotalSystemMemory returns the total physical memory of the system in bytes
func getTotalSystemMemory() int64 {
	var totalMemory int64

	// Try sysctl for BSD-based systems (macOS, FreeBSD, NetBSD, OpenBSD)
	if runtime.GOOS == "darwin" || runtime.GOOS == "freebsd" ||
	   runtime.GOOS == "netbsd" || runtime.GOOS == "openbsd" {
		// Use hw.memsize for macOS, hw.physmem for FreeBSD/NetBSD/OpenBSD
		var sysctlKey string
		if runtime.GOOS == "darwin" {
			sysctlKey = "hw.memsize"
		} else {
			sysctlKey = "hw.physmem"
		}

		cmd := exec.Command("sysctl", "-n", sysctlKey)
		output, err := cmd.Output()
		if err == nil {
			memStr := strings.TrimSpace(string(output))
			mem, err := strconv.ParseInt(memStr, 10, 64)
			if err == nil {
				totalMemory = mem
			}
		}
	} else if runtime.GOOS == "linux" {
		// For Linux, use /proc/meminfo
		cmd := exec.Command("grep", "MemTotal", "/proc/meminfo")
		output, err := cmd.Output()
		if err == nil {
			memStr := strings.TrimSpace(string(output))
			// Format is: "MemTotal:       16384516 kB"
			fields := strings.Fields(memStr)
			if len(fields) >= 2 {
				// Convert from KB to bytes
				mem, err := strconv.ParseInt(fields[1], 10, 64)
				if err == nil {
					totalMemory = mem * 1024 // Convert KB to bytes
				}
			}
		}
	} else {
		// For other POSIX systems, try the generic 'free' command
		cmd := exec.Command("free", "-b")
		output, err := cmd.Output()
		if err == nil {
			lines := strings.Split(string(output), "\n")
			if len(lines) > 1 {
				// Parse the second line which contains memory info
				fields := strings.Fields(lines[1])
				if len(fields) > 1 {
					mem, err := strconv.ParseInt(fields[1], 10, 64)
					if err == nil {
						totalMemory = mem
					}
				}
			}
		}
	}

	// Fallback if we couldn't get system memory or on unsupported platforms
	if totalMemory <= 0 {
		// Use runtime memory stats as fallback
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		totalMemory = int64(mem.TotalAlloc)

		// Set a reasonable minimum if we couldn't determine actual memory
		if totalMemory < 1<<30 { // 1 GB
			totalMemory = 1 << 30
		}
	}

	return totalMemory
}

// startBackgroundWorker starts a background goroutine that listens for commands on the workerChannel
func (db *DB) startBackgroundWorker() {
	// Add 1 to the wait group before starting the goroutine
	db.workerWaitGroup.Add(1)

	go func() {
		// Ensure the wait group is decremented when the goroutine exits
		defer db.workerWaitGroup.Done()

		// Timer for periodic flush (15 seconds)
		flushInterval := 15 * time.Second
		timer := time.NewTimer(flushInterval)
		defer timer.Stop()

		for {
			select {
			case cmd, ok := <-db.workerChannel:
				if !ok {
					// Channel closed, exit
					return
				}

				switch cmd {
				case "flush":
					// Coordinate with Close() using readMutex
					db.readMutex.RLock()
					db.flushIndexToDisk()
					db.readMutex.RUnlock()
					// Clear the pending command flag
					db.seqMutex.Lock()
					delete(db.pendingCommands, "flush")
					db.seqMutex.Unlock()

					// Reset timer after manual flush
					if !timer.Stop() {
						<-timer.C
					}
					timer.Reset(flushInterval)

				case "clean":
					// Coordinate with Close() using readMutex
					db.readMutex.RLock()
					// Discard previous versions of pages
					numPages := db.discardOldPageVersions(true)
					// If the number of pages is still greater than the cache size threshold
					if numPages > db.cacheSizeThreshold {
						// Remove old pages from cache
						db.removeOldPagesFromCache()
					}
					db.readMutex.RUnlock()
					// Clear the pending command flag
					db.seqMutex.Lock()
					delete(db.pendingCommands, "clean")
					db.seqMutex.Unlock()

				case "clean_values":
					// Coordinate with Close() using readMutex
					db.readMutex.RLock()
					// Clean up old value cache entries
					db.cleanupOldValueCacheEntries()
					db.readMutex.RUnlock()
					// Clear the pending command flag
					db.seqMutex.Lock()
					delete(db.pendingCommands, "clean_values")
					db.seqMutex.Unlock()

				case "checkpoint":
					// Coordinate with Close() using readMutex
					db.readMutex.RLock()
					db.checkpointWAL()
					db.readMutex.RUnlock()
					// Clear the pending command flag
					db.seqMutex.Lock()
					delete(db.pendingCommands, "checkpoint")
					db.seqMutex.Unlock()

				case "exit":
					return

				default:
					debugPrint("Unknown worker command: %s\n", cmd)
				}

			case <-timer.C:
				// Timer expired, check if we need to flush
				timeSinceLastFlush := time.Since(db.lastFlushTime)
				if timeSinceLastFlush >= flushInterval {
					// Only flush if there are dirty pages or if it's been a while
					if db.dirtyPageCount > 0 {
						debugPrint("Timer-based flush triggered after %v\n", timeSinceLastFlush)
						// Coordinate with Close() using readMutex
						db.readMutex.RLock()
						db.flushIndexToDisk()
						db.readMutex.RUnlock()
					}
				}
				// Reset timer for next interval
				timer.Reset(flushInterval)
			}
		}
	}()
}

// ------------------------------------------------------------------------------------------------
// Hash Table Functions
// ------------------------------------------------------------------------------------------------

// hashKey hashes the key with the given salt using FNV-1a hash
func hashKey(key []byte, salt uint8) uint64 {
	// Simple FNV-1a hash implementation
	hash := uint64(14695981039346656037)

	// Process the salt
	hash ^= uint64(salt)
	hash *= 1099511628211

	// Process the key
	for _, b := range key {
		hash ^= uint64(b)
		hash *= 1099511628211
	}

	return hash
}

// getTableSlot calculates the slot for a given key in a table page
func (db *DB) getTableSlot(key []byte, salt uint8) int {
	// Hash the key with the salt
	hash := hashKey(key, salt)
	// Calculate slot in the table page
	return int(hash % uint64(TableEntries))
}

// generateNewSalt generates a new salt that's different from the old one
func generateNewSalt(oldSalt uint8) uint8 {
	return oldSalt + 1
}

// findNonCollidingSalt finds a salt that avoids collisions between the given entries
func (db *DB) findNonCollidingSalt(parentSalt uint8, entries []HybridEntry) (uint8, error) {
	salt := generateNewSalt(parentSalt)

	if len(entries) <= 1 {
		// No collision possible with one or zero entries
		return salt, nil
	}

	// Use an array to track used slots
	var usedSlots [TableEntries]byte

	for salt <= 255 {
		// Check if any entries have the same slot with this salt
		hasCollision := false

		for _, entry := range entries {
			slot := db.getTableSlot(entry.Key, salt)
			if usedSlots[slot] != 0 {
				hasCollision = true
				break
			}
			usedSlots[slot] = 1
		}

		// If no collision found, we can use this salt
		if !hasCollision {
			return salt, nil
		}

		// Clear the array for the next salt iteration
		usedSlots = [TableEntries]byte{}

		// If we couldn't find a non-colliding salt (very unlikely)
		if salt == 255 {
			return 0, fmt.Errorf("could not find a salt that avoids collisions for %d entries", len(entries))
		}
		// Try next salt
		salt = generateNewSalt(salt)
	}

	return 0, fmt.Errorf("could not find a salt that avoids collisions for %d entries", len(entries))
}
