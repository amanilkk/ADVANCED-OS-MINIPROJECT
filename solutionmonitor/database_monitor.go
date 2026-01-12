package solutionmonitor

import (
	"fmt"
	"sync"
	"time"
)

// ================= MONITOR DATABASE =================

// Transaction keeps track of ID, start time, and operations performed
type Transaction struct {
	ID         int
	StartTime  time.Time
	Operations []string
}

// Record stores a key/value pair with versioning and last update time
type Record struct {
	Key       string
	Value     int
	Version   int
	UpdatedAt time.Time
}

// Stats for database activity and potential inconsistencies
type Stats struct {
	TotalReads     int
	TotalWrites    int
	TotalUpdates   int
	LostUpdates    int
	DataCorruption int
}

// Database with monitor-style locking
// Supports per-key locks and single active transaction enforcement
type Database struct {
	mu        sync.Mutex
	cond      *sync.Cond
	records   map[string]*Record
	txCounter int
	stats     Stats
	busy      bool            // true if a transaction is active
	locked    map[string]bool // per-key locks
}

// Constructor
func NewDatabase() *Database {
	db := &Database{
		records: make(map[string]*Record),
		locked:  make(map[string]bool),
	}
	db.cond = sync.NewCond(&db.mu) // initialize condition variable for monitor
	return db
}

// ================= KEY LOCKING =================

// acquireKey blocks until a specific key is free
func (db *Database) acquireKey(key string) {
	for db.locked[key] {
		db.cond.Wait() // wait until key is released
	}
	db.locked[key] = true
}

// releaseKey frees a specific key and wakes waiting goroutines
func (db *Database) releaseKey(key string) {
	db.locked[key] = false
	db.cond.Broadcast() // wake up any goroutines waiting for this key
}

// ================= TRANSACTIONS =================

// BeginTransaction waits until no other transaction is active
func (db *Database) BeginTransaction() *Transaction {
	db.mu.Lock()
	for db.busy {
		db.cond.Wait() // wait until no active transaction
	}
	db.busy = true

	db.txCounter++
	tx := &Transaction{
		ID:         db.txCounter,
		StartTime:  time.Now(),
		Operations: []string{},
	}
	db.mu.Unlock()
	return tx
}

// Commit releases the transaction lock and records the commit
func (db *Database) Commit(tx *Transaction) {
	duration := time.Since(tx.StartTime)
	db.mu.Lock()
	tx.Operations = append(tx.Operations, fmt.Sprintf("COMMIT (duration: %v)", duration))
	db.busy = false
	db.cond.Broadcast() // wake waiting transactions
	db.mu.Unlock()
}

// Abort releases the transaction lock if something goes wrong
func (db *Database) Abort(tx *Transaction) {
	duration := time.Since(tx.StartTime)
	db.mu.Lock()
	tx.Operations = append(tx.Operations, fmt.Sprintf("ABORT (duration: %v)", duration))
	db.busy = false
	db.cond.Broadcast()
	db.mu.Unlock()
}

// ================= CRUD OPERATIONS =================

// Read safely reads a record under a per-key lock
func (db *Database) Read(tx *Transaction, key string) (int, bool) {
	db.mu.Lock()
	db.acquireKey(key) // ensure exclusive access to key
	db.stats.TotalReads++

	record, exists := db.records[key]
	if exists {
		tx.Operations = append(tx.Operations, fmt.Sprintf("READ %s: %d", key, record.Value))
	} else {
		tx.Operations = append(tx.Operations, fmt.Sprintf("READ %s: NOT_FOUND", key))
	}

	db.releaseKey(key)
	db.mu.Unlock()

	if !exists {
		return 0, false
	}
	return record.Value, true
}

// Write safely writes or creates a record
func (db *Database) Write(tx *Transaction, key string, value int) {
	db.mu.Lock()
	db.acquireKey(key)
	db.stats.TotalWrites++

	if record, exists := db.records[key]; exists {
		record.Value = value
		record.Version++
		record.UpdatedAt = time.Now()
		tx.Operations = append(tx.Operations, fmt.Sprintf("WRITE %s: %d (v%d)", key, value, record.Version))
	} else {
		db.records[key] = &Record{
			Key:       key,
			Value:     value,
			Version:   1,
			UpdatedAt: time.Now(),
		}
		tx.Operations = append(tx.Operations, fmt.Sprintf("WRITE %s: %d (new)", key, value))
	}

	db.releaseKey(key)
	db.mu.Unlock()
}

// Update safely increments a record's value
func (db *Database) Update(tx *Transaction, key string, delta int) bool {
	db.mu.Lock()
	db.acquireKey(key)
	db.stats.TotalUpdates++

	record, exists := db.records[key]
	if !exists {
		tx.Operations = append(tx.Operations, fmt.Sprintf("UPDATE %s: NOT_FOUND", key))
		db.releaseKey(key)
		db.mu.Unlock()
		return false
	}

	record.Value += delta
	record.Version++
	record.UpdatedAt = time.Now()
	tx.Operations = append(tx.Operations, fmt.Sprintf("UPDATE %s: %+d = %d (v%d)", key, delta, record.Value, record.Version))

	db.releaseKey(key)
	db.mu.Unlock()
	return true
}

// Delete safely removes a record
func (db *Database) Delete(tx *Transaction, key string) bool {
	db.mu.Lock()
	db.acquireKey(key)

	_, exists := db.records[key]
	if !exists {
		tx.Operations = append(tx.Operations, fmt.Sprintf("DELETE %s: NOT_FOUND", key))
		db.releaseKey(key)
		db.mu.Unlock()
		return false
	}

	delete(db.records, key)
	tx.Operations = append(tx.Operations, fmt.Sprintf("DELETE %s: SUCCESS", key))
	db.releaseKey(key)
	db.mu.Unlock()
	return true
}

// ================= UTILITIES =================

// GetStats returns a snapshot of current database statistics
func (db *Database) GetStats() Stats {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.stats
}

// PrintStats prints the database statistics
func (db *Database) PrintStats() {
	stats := db.GetStats()
	fmt.Println("\n=== Database Statistics ===")
	fmt.Printf("Total Reads: %d\n", stats.TotalReads)
	fmt.Printf("Total Writes: %d\n", stats.TotalWrites)
	fmt.Printf("Total Updates: %d\n", stats.TotalUpdates)
	fmt.Printf("Lost Updates: %d\n", stats.LostUpdates)
	fmt.Printf("Data Corruption: %d\n", stats.DataCorruption)
	fmt.Println("===========================")
}

// GetRecordCount returns the number of records
func (db *Database) GetRecordCount() int {
	db.mu.Lock()
	defer db.mu.Unlock()
	return len(db.records)
}

// PrintRecords prints all records with value, version, and timestamp
func (db *Database) PrintRecords() {
	db.mu.Lock()
	defer db.mu.Unlock()

	fmt.Println("\n=== Database Records ===")
	for key, record := range db.records {
		fmt.Printf("%s: value=%d, version=%d, updated=%v\n",
			key, record.Value, record.Version,
			record.UpdatedAt.Format("15:04:05.000"))
	}
	fmt.Println("========================")
}

// VerifyIntegrity checks all records against expected values
func (db *Database) VerifyIntegrity(expectedValues map[string]int) (bool, []string) {
	db.mu.Lock()
	defer db.mu.Unlock()

	errors := make([]string, 0)
	for key, expectedValue := range expectedValues {
		record, exists := db.records[key]
		if !exists {
			errors = append(errors, fmt.Sprintf("Key %s missing (expected %d)", key, expectedValue))
			continue
		}
		if record.Value != expectedValue {
			errors = append(errors, fmt.Sprintf("Key %s has value %d (expected %d)", key, record.Value, expectedValue))
			db.stats.DataCorruption++
		}
	}
	return len(errors) == 0, errors
}
