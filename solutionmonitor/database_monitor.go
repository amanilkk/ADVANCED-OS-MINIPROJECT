package solutionmonitor

import (
	"fmt"
	"sync"
	"time"
)
type Transaction struct {
	ID         int
	StartTime  time.Time
	Operations []string
}

// Record struct (internal)
type Record struct {
	Key       string
	Value     int
	Version   int
	UpdatedAt time.Time
}

// Stats for monitoring
type Stats struct {
	TotalReads     int
	TotalWrites    int
	TotalUpdates   int
	LostUpdates    int
	DataCorruption int
}
// Record represents a single database record

// Database represents an in-memory key-value database
// WARNING: This implementation has NO synchronization!
// Multiple goroutines accessing this will cause race conditions.
type Database struct {
	mu        sync.Mutex
	cond      *sync.Cond
	records   map[string]*Record
	txCounter int
	activeTx  bool
	stats     Stats
}

// Stats tracks database statistics to detect corruption

// NewDatabase creates a new database instance
func NewDatabase() *Database {
	db := &Database{
		records: make(map[string]*Record),
	}
	db.cond = sync.NewCond(&db.mu)
	return db
}

// BeginTransaction starts a new transaction
// RACE CONDITION: txCounter is not protected!
func (db *Database) BeginTransaction() *Transaction {
	db.mu.Lock()

	for db.activeTx {
		db.cond.Wait()
	}

	db.activeTx = true
	db.txCounter++

	return &Transaction{
		ID:        db.txCounter,
		StartTime: time.Now(),
	}
}

// Read retrieves a value from the database
// RACE CONDITION: Reading while another goroutine is writing
func (db *Database) Read(tx *Transaction, key string) (int, bool) {
	db.stats.TotalReads++ // UNSAFE: Not atomic

	record, exists := db.records[key]
	if !exists {
		tx.Operations = append(tx.Operations, fmt.Sprintf("READ %s: NOT_FOUND", key))
		return 0, false
	}

	// Simulate some processing time to increase likelihood of race conditions
	time.Sleep(time.Microsecond * 10)

	value := record.Value // UNSAFE: Value might change between check and read
	tx.Operations = append(tx.Operations, fmt.Sprintf("READ %s: %d", key, value))
	return value, true
}

// Write creates or updates a record in the database
// RACE CONDITION: Multiple writes to the same key can cause lost updates
func (db *Database) Write(tx *Transaction, key string, value int) {
	db.stats.TotalWrites++ // UNSAFE: Not atomic

	existingRecord, exists := db.records[key]

	// Simulate some processing time
	time.Sleep(time.Microsecond * 10)

	if exists {
		// UNSAFE: Another goroutine might update version between read and write
		oldVersion := existingRecord.Version
		existingRecord.Value = value
		existingRecord.Version = oldVersion + 1 // Lost update can happen here!
		existingRecord.UpdatedAt = time.Now()
		tx.Operations = append(tx.Operations, fmt.Sprintf("WRITE %s: %d (v%d)", key, value, existingRecord.Version))
	} else {
		// UNSAFE: Two goroutines might both think the key doesn't exist
		db.records[key] = &Record{
			Key:       key,
			Value:     value,
			Version:   1,
			UpdatedAt: time.Now(),
		}
		tx.Operations = append(tx.Operations, fmt.Sprintf("WRITE %s: %d (new)", key, value))
	}
}

// Update performs a read-modify-write operation
// RACE CONDITION: Classic lost update problem!
func (db *Database) Update(tx *Transaction, key string, delta int) bool {
	db.stats.TotalUpdates++ // UNSAFE: Not atomic

	// Read current value
	currentValue, exists := db.records[key]
	if !exists {
		tx.Operations = append(tx.Operations, fmt.Sprintf("UPDATE %s: NOT_FOUND", key))
		return false
	}

	// Simulate some processing time (makes race condition more likely)
	time.Sleep(time.Microsecond * 50)

	// UNSAFE: Another goroutine might have modified the value!
	oldVersion := currentValue.Version
	newValue := currentValue.Value + delta
	currentValue.Value = newValue
	currentValue.Version = oldVersion + 1
	currentValue.UpdatedAt = time.Now()

	tx.Operations = append(tx.Operations, fmt.Sprintf("UPDATE %s: +%d = %d (v%d)", key, delta, newValue, currentValue.Version))
	return true
}

// Delete removes a record from the database
// RACE CONDITION: Concurrent deletes or delete during read
func (db *Database) Delete(tx *Transaction, key string) bool {
	_, exists := db.records[key]
	if !exists {
		tx.Operations = append(tx.Operations, fmt.Sprintf("DELETE %s: NOT_FOUND", key))
		return false
	}

	// Simulate some processing time
	time.Sleep(time.Microsecond * 10)

	// UNSAFE: Another goroutine might delete or modify this key
	delete(db.records, key)
	tx.Operations = append(tx.Operations, fmt.Sprintf("DELETE %s: SUCCESS", key))
	return true
}

// Commit finalizes a transaction
func (db *Database) Commit(tx *Transaction) {
	duration := time.Since(tx.StartTime)
	tx.Operations = append(tx.Operations, fmt.Sprintf("COMMIT (duration: %v)", duration))
	db.activeTx = false
	db.cond.Signal()
	db.mu.Unlock()
}

// Abort cancels a transaction
func (db *Database) Abort(tx *Transaction) {
	duration := time.Since(tx.StartTime)
	tx.Operations = append(tx.Operations, fmt.Sprintf("ABORT (duration: %v)", duration))

	db.activeTx = false
	db.cond.Signal()
	db.mu.Unlock()
}

// GetStats returns current database statistics
// RACE CONDITION: Stats are being read while being modified
func (db *Database) GetStats() Stats {
	return db.stats // UNSAFE: Struct copy is not atomic
}

// VerifyIntegrity checks for data corruption
// This helps demonstrate that race conditions occurred
func (db *Database) VerifyIntegrity(expectedValues map[string]int) (bool, []string) {
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

// PrintStats displays database statistics
func (db *Database) PrintStats() {
	stats := db.GetStats()
	fmt.Println("\n=== Database Statistics ===")
	fmt.Printf("Total Reads:     %d\n", stats.TotalReads)
	fmt.Printf("Total Writes:    %d\n", stats.TotalWrites)
	fmt.Printf("Total Updates:   %d\n", stats.TotalUpdates)
	fmt.Printf("Lost Updates:    %d\n", stats.LostUpdates)
	fmt.Printf("Data Corruption: %d\n", stats.DataCorruption)
	fmt.Println("===========================")
}

// GetRecordCount returns the number of records
// RACE CONDITION: Map length can change during iteration
func (db *Database) GetRecordCount() int {
	return len(db.records) // UNSAFE: Map access not synchronized
}

// PrintRecords displays all records (for debugging)
// RACE CONDITION: Iterating over map while it's being modified
func (db *Database) PrintRecords() {
	fmt.Println("\n=== Database Records ===")
	for key, record := range db.records { // UNSAFE: Concurrent map iteration
		fmt.Printf("%s: value=%d, version=%d, updated=%v\n",
			key, record.Value, record.Version, record.UpdatedAt.Format("15:04:05.000"))
	}
	fmt.Println("========================")
}
