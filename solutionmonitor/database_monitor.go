package solutionmonitor

import (
	"fmt"
	"sync"
	"time"
)

/*
APPROACH 2: MONITOR PATTERN (SINGLE ACTIVE TRANSACTION)
  
Description:
- This implementation enforces that only one transaction is active at a time.
- Uses a **mutex (`db.mu`)** and a **condition variable (`db.cond`)** to serialize transactions.
- Transactions that try to start while another is active will **wait on the condition variable**.
- Once the active transaction commits or aborts, waiting transactions are signaled.
- This is a form of a monitor: mutual exclusion + condition synchronization.
*/

type Transaction struct {
	ID         int
	StartTime  time.Time
	Operations []string
}

// Record represents a single database record
type Record struct {
	Key       string
	Value     int
	Version   int
	UpdatedAt time.Time
}

// Stats tracks database operations
type Stats struct {
	TotalReads     int
	TotalWrites    int
	TotalUpdates   int
	LostUpdates    int
	DataCorruption int
}

// Database represents an in-memory key-value database
type Database struct {
	mu        sync.Mutex    // Mutex protects the monitor state (activeTx, txCounter)
	cond      *sync.Cond    // Condition variable to wait for transaction availability
	records   map[string]*Record
	txCounter int
	activeTx  bool           // Indicates if a transaction is currently active
	stats     Stats
}

// NewDatabase initializes a new database and monitor
func NewDatabase() *Database {
	db := &Database{
		records: make(map[string]*Record),
	}
	db.cond = sync.NewCond(&db.mu)
	return db
}

/* ================= TRANSACTIONS ================= */

// BeginTransaction blocks if another transaction is active
func (db *Database) BeginTransaction() *Transaction {
	db.mu.Lock()          // Enter monitor
	for db.activeTx {     // Wait until no active transaction
		db.cond.Wait()    // Releases mutex while waiting, reacquires on wake-up
	}

	db.activeTx = true    // Mark this transaction as active
	db.txCounter++        // Assign a unique ID safely within mutex

	return &Transaction{
		ID:        db.txCounter,
		StartTime: time.Now(),
	}
}

// Commit releases the active transaction and signals waiting goroutines
func (db *Database) Commit(tx *Transaction) {
	duration := time.Since(tx.StartTime)
	tx.Operations = append(tx.Operations, fmt.Sprintf("COMMIT (duration: %v)", duration))

	db.activeTx = false     // Release the monitor
	db.cond.Signal()        // Wake up one waiting transaction
	db.mu.Unlock()          // Exit monitor
}

// Abort cancels a transaction and signals waiting goroutines
func (db *Database) Abort(tx *Transaction) {
	duration := time.Since(tx.StartTime)
	tx.Operations = append(tx.Operations, fmt.Sprintf("ABORT (duration: %v)", duration))

	db.activeTx = false
	db.cond.Signal()
	db.mu.Unlock()
}

/* ================= CRUD OPERATIONS ================= */

// All CRUD operations in this monitor are NOT fully synchronized individually.
// They rely on the single active transaction enforcement to avoid races.
// Sleep calls simulate processing and make race conditions more likely
// if multiple goroutines bypass the monitor incorrectly.

// Read operation
func (db *Database) Read(tx *Transaction, key string) (int, bool) {
	db.stats.TotalReads++ // NOT atomic
	record, exists := db.records[key]
	if !exists {
		tx.Operations = append(tx.Operations, fmt.Sprintf("READ %s: NOT_FOUND", key))
		return 0, false
	}

	time.Sleep(time.Microsecond * 10) // Simulate delay
	value := record.Value              // Could be unsafe if multiple writes occur
	tx.Operations = append(tx.Operations, fmt.Sprintf("READ %s: %d", key, value))
	return value, true
}

// Write operation
func (db *Database) Write(tx *Transaction, key string, value int) {
	db.stats.TotalWrites++ // NOT atomic

	existingRecord, exists := db.records[key]
	time.Sleep(time.Microsecond * 10)

	if exists {
		oldVersion := existingRecord.Version
		existingRecord.Value = value
		existingRecord.Version = oldVersion + 1
		existingRecord.UpdatedAt = time.Now()
		tx.Operations = append(tx.Operations, fmt.Sprintf("WRITE %s: %d (v%d)", key, value, existingRecord.Version))
	} else {
		db.records[key] = &Record{
			Key:       key,
			Value:     value,
			Version:   1,
			UpdatedAt: time.Now(),
		}
		tx.Operations = append(tx.Operations, fmt.Sprintf("WRITE %s: %d (new)", key, value))
	}
}

// Update operation (read-modify-write)
func (db *Database) Update(tx *Transaction, key string, delta int) bool {
	db.stats.TotalUpdates++
	currentValue, exists := db.records[key]
	if !exists {
		tx.Operations = append(tx.Operations, fmt.Sprintf("UPDATE %s: NOT_FOUND", key))
		return false
	}

	time.Sleep(time.Microsecond * 50)
	oldVersion := currentValue.Version
	newValue := currentValue.Value + delta
	currentValue.Value = newValue
	currentValue.Version = oldVersion + 1
	currentValue.UpdatedAt = time.Now()

	tx.Operations = append(tx.Operations, fmt.Sprintf("UPDATE %s: +%d = %d (v%d)", key, delta, newValue, currentValue.Version))
	return true
}

// Delete operation
func (db *Database) Delete(tx *Transaction, key string) bool {
	_, exists := db.records[key]
	if !exists {
		tx.Operations = append(tx.Operations, fmt.Sprintf("DELETE %s: NOT_FOUND", key))
		return false
	}

	time.Sleep(time.Microsecond * 10)
	delete(db.records, key)
	tx.Operations = append(tx.Operations, fmt.Sprintf("DELETE %s: SUCCESS", key))
	return true
}

/* ================= UTILITIES ================= */

// GetStats returns database stats (unsafe if called concurrently)
func (db *Database) GetStats() Stats {
	return db.stats
}

// VerifyIntegrity checks for data corruption (demonstrates race conditions)
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

// PrintStats prints current database statistics
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

// GetRecordCount returns record count (unsafe if concurrent)
func (db *Database) GetRecordCount() int {
	return len(db.records)
}

// PrintRecords displays all records (unsafe for concurrent access)
func (db *Database) PrintRecords() {
	fmt.Println("\n=== Database Records ===")
	for key, record := range db.records {
		fmt.Printf("%s: value=%d, version=%d, updated=%v\n",
			key, record.Value, record.Version, record.UpdatedAt.Format("15:04:05.000"))
	}
	fmt.Println("========================")
}
