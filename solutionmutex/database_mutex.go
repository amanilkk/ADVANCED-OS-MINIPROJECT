package solutionmutex

import (
	"fmt"
	"sync"
	"time"
)

/*
APPROACH 1: COARSE-GRAINED MUTEX (TRANSACTION-LEVEL)
*/

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
type Database struct {
	mu        sync.Mutex
	records   map[string]*Record
	txCounter int
	stats     Stats
}

func NewDatabase() *Database {
	return &Database{
		records: make(map[string]*Record),
	}
}

/* ================= TRANSACTIONS ================= */

func (db *Database) BeginTransaction() *Transaction {
	db.mu.Lock()

	db.txCounter++
	return &Transaction{
		ID:         db.txCounter,
		StartTime:  time.Now(),
		Operations: []string{},
	}
}

func (db *Database) Commit(tx *Transaction) {
	duration := time.Since(tx.StartTime)
	tx.Operations = append(tx.Operations,
		fmt.Sprintf("COMMIT (duration: %v)", duration))

	db.mu.Unlock()
}

func (db *Database) Abort(tx *Transaction) {
	duration := time.Since(tx.StartTime)
	tx.Operations = append(tx.Operations,
		fmt.Sprintf("ABORT (duration: %v)", duration))

	db.mu.Unlock()
}

/* ================= CRUD OPERATIONS ================= */

func (db *Database) Read(tx *Transaction, key string) (int, bool) {
	db.stats.TotalReads++

	record, exists := db.records[key]
	if !exists {
		tx.Operations = append(tx.Operations,
			fmt.Sprintf("READ %s: NOT_FOUND", key))
		return 0, false
	}

	tx.Operations = append(tx.Operations,
		fmt.Sprintf("READ %s: %d", key, record.Value))
	return record.Value, true
}

func (db *Database) Write(tx *Transaction, key string, value int) {
	db.stats.TotalWrites++

	if record, exists := db.records[key]; exists {
		record.Value = value
		record.Version++
		record.UpdatedAt = time.Now()
		tx.Operations = append(tx.Operations,
			fmt.Sprintf("WRITE %s: %d (v%d)", key, value, record.Version))
	} else {
		db.records[key] = &Record{
			Key:       key,
			Value:     value,
			Version:   1,
			UpdatedAt: time.Now(),
		}
		tx.Operations = append(tx.Operations,
			fmt.Sprintf("WRITE %s: %d (new)", key, value))
	}
}

func (db *Database) Update(tx *Transaction, key string, delta int) bool {
	db.stats.TotalUpdates++

	record, exists := db.records[key]
	if !exists {
		tx.Operations = append(tx.Operations,
			fmt.Sprintf("UPDATE %s: NOT_FOUND", key))
		return false
	}

	record.Value += delta
	record.Version++
	record.UpdatedAt = time.Now()

	tx.Operations = append(tx.Operations,
		fmt.Sprintf("UPDATE %s: %+d = %d (v%d)",
			key, delta, record.Value, record.Version))

	return true
}

func (db *Database) Delete(tx *Transaction, key string) bool {
	if _, exists := db.records[key]; !exists {
		tx.Operations = append(tx.Operations,
			fmt.Sprintf("DELETE %s: NOT_FOUND", key))
		return false
	}

	delete(db.records, key)
	tx.Operations = append(tx.Operations,
		fmt.Sprintf("DELETE %s: SUCCESS", key))
	return true
}

/* ================= UTILITIES ================= */

func (db *Database) GetStats() Stats {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.stats
}

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

func (db *Database) GetRecordCount() int {
	db.mu.Lock()
	defer db.mu.Unlock()
	return len(db.records)
}

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

// ADD THIS METHOD - Required by DatabaseInterface
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
