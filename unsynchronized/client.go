package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// ClientConfig defines behavior for a simulated client
type ClientConfig struct {
	ID              int
	NumTransactions int
	OperationsPerTx int
	ThinkTime       time.Duration // Time between operations
}

// Client simulates a database client performing transactions
type Client struct {
	config ClientConfig
	db     *Database
	rng    *rand.Rand
}

// NewClient creates a new client instance
func NewClient(config ClientConfig, db *Database) *Client {
	return &Client{
		config: config,
		db:     db,
		rng:    rand.New(rand.NewSource(time.Now().UnixNano() + int64(config.ID))),
	}
}

// Run executes the client's workload
// This will be called as a goroutine, causing concurrent access to the database
func (c *Client) Run(wg *sync.WaitGroup) {
	defer wg.Done()

	for i := 0; i < c.config.NumTransactions; i++ {
		c.executeTransaction(i)

		// Small delay between transactions
		if c.config.ThinkTime > 0 {
			time.Sleep(c.config.ThinkTime)
		}
	}
}

// executeTransaction performs a single transaction with multiple operations
func (c *Client) executeTransaction(txNum int) {
	tx := c.db.BeginTransaction()

	// Perform random operations
	for i := 0; i < c.config.OperationsPerTx; i++ {
		c.performRandomOperation(tx)
	}

	// Commit the transaction
	c.db.Commit(tx)
}

// performRandomOperation executes a random database operation
func (c *Client) performRandomOperation(tx *Transaction) {
	operation := c.rng.Intn(4) // 0: Read, 1: Write, 2: Update, 3: Delete

	// Use a small set of keys to increase contention
	keys := []string{"account_1", "account_2", "account_3", "counter", "balance"}
	key := keys[c.rng.Intn(len(keys))]

	switch operation {
	case 0: // Read
		c.db.Read(tx, key)

	case 1: // Write
		value := c.rng.Intn(1000)
		c.db.Write(tx, key, value)

	case 2: // Update (most likely to cause race conditions)
		delta := c.rng.Intn(100) - 50 // Random delta between -50 and 50
		c.db.Update(tx, key, delta)

	case 3: // Delete (occasionally)
		if c.rng.Float32() < 0.1 { // Only 10% chance to delete
			c.db.Delete(tx, key)
		}
	}
}

// RunBankTransferScenario simulates the classic bank transfer problem
// This demonstrates the lost update problem clearly
func RunBankTransferScenario(db *Database, numClients int, transfersPerClient int) {
	fmt.Println("\n=== Bank Transfer Scenario ===")
	fmt.Printf("Running %d clients, each performing %d transfers\n", numClients, transfersPerClient)

	// Initialize two accounts with 1000 each
	initTx := db.BeginTransaction()
	db.Write(initTx, "account_A", 1000)
	db.Write(initTx, "account_B", 1000)
	db.Commit(initTx)

	initialTotal := 2000
	fmt.Printf("Initial state: account_A=1000, account_B=1000, total=%d\n", initialTotal)

	var wg sync.WaitGroup

	// Each client will transfer money between accounts
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		clientID := i

		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(clientID)))

			for j := 0; j < transfersPerClient; j++ {
				amount := rng.Intn(50) + 1 // Transfer 1-50

				// Transfer from A to B
				tx := db.BeginTransaction()

				// Read from account A
				balanceA, _ := db.Read(tx, "account_A")

				// Simulate processing time
				time.Sleep(time.Microsecond * 100)

				// Read from account B
				balanceB, _ := db.Read(tx, "account_B")

				// Update both accounts (RACE CONDITION!)
				db.Write(tx, "account_A", balanceA-amount)
				db.Write(tx, "account_B", balanceB+amount)

				db.Commit(tx)
			}
		}()
	}

	wg.Wait()

	// Verify total is still 2000 (it won't be due to race conditions!)
tx := db.BeginTransaction()
finalA, _ := db.Read(tx, "account_A")
finalB, _ := db.Read(tx, "account_B")
db.Commit(tx)


	finalTotal := finalA + finalB

	fmt.Printf("\nFinal state: account_A=%d, account_B=%d, total=%d\n", finalA, finalB, finalTotal)

	if finalTotal != initialTotal {
		fmt.Printf("❌ RACE CONDITION DETECTED! Lost %d in total (expected %d, got %d)\n",
			initialTotal-finalTotal, initialTotal, finalTotal)
	} else {
		fmt.Printf("✓ Total preserved (got lucky, or not enough contention)\n")
	}
}

// RunCounterScenario simulates multiple clients incrementing a shared counter
// This clearly demonstrates the lost update problem
func RunCounterScenario(db *Database, numClients int, incrementsPerClient int) {
	fmt.Println("\n=== Counter Increment Scenario ===")
	fmt.Printf("Running %d clients, each incrementing %d times\n", numClients, incrementsPerClient)

	// Initialize counter to 0
	initTx := db.BeginTransaction()
	db.Write(initTx, "counter", 0)
	db.Commit(initTx)

	expectedFinal := numClients * incrementsPerClient
	fmt.Printf("Expected final value: %d\n", expectedFinal)

	var wg sync.WaitGroup

	// Each client increments the counter
	for i := 0; i < numClients; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for j := 0; j < incrementsPerClient; j++ {
				tx := db.BeginTransaction()
				db.Update(tx, "counter", 1) // Increment by 1
				db.Commit(tx)
			}
		}()
	}

	wg.Wait()

	tx := db.BeginTransaction()
finalValue, _ := db.Read(tx, "counter")
db.Commit(tx)

	fmt.Printf("Final counter value: %d\n", finalValue)

	if finalValue != expectedFinal {
		lostUpdates := expectedFinal - finalValue
		fmt.Printf("❌ RACE CONDITION DETECTED! Lost %d updates (%.1f%% lost)\n",
			lostUpdates, float64(lostUpdates)/float64(expectedFinal)*100)
	} else {
		fmt.Printf("✓ All updates recorded (got lucky, or not enough contention)\n")
	}
}

// RunReadWriteScenario demonstrates dirty reads and inconsistent reads
func RunReadWriteScenario(db *Database, numReaders int, numWriters int, duration time.Duration) {
	fmt.Println("\n=== Read-Write Scenario ===")
	fmt.Printf("Running %d readers and %d writers for %v\n", numReaders, numWriters, duration)

	// Initialize some data
	initTx := db.BeginTransaction()
	db.Write(initTx, "data_1", 100)
	db.Write(initTx, "data_2", 100)
	db.Commit(initTx)

	stopChan := make(chan bool)
	var wg sync.WaitGroup

	inconsistentReads := 0
	var inconsistentMutex sync.Mutex

	// Start readers
	for i := 0; i < numReaders; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for {
				select {
				case <-stopChan:
					return
				default:
					tx := db.BeginTransaction()
					val1, _ := db.Read(tx, "data_1")
					val2, _ := db.Read(tx, "data_2")

					// These should always be equal, but won't be due to race conditions
					if val1 != val2 {
						inconsistentMutex.Lock()
						inconsistentReads++
						inconsistentMutex.Unlock()
					}

					db.Commit(tx)
					time.Sleep(time.Microsecond * 100)
				}
			}
		}()
	}

	// Start writers (they keep data_1 and data_2 synchronized)
	for i := 0; i < numWriters; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano()))

			for {
				select {
				case <-stopChan:
					return
				default:
					tx := db.BeginTransaction()
					newValue := rng.Intn(1000)

					// Write same value to both (should be atomic, but isn't!)
					db.Write(tx, "data_1", newValue)
					time.Sleep(time.Microsecond * 50) // Increase chance of inconsistent read
					db.Write(tx, "data_2", newValue)

					db.Commit(tx)
					time.Sleep(time.Microsecond * 100)
				}
			}
		}()
	}

	// Run for specified duration
	time.Sleep(duration)
	close(stopChan)
	wg.Wait()

	fmt.Printf("\nInconsistent reads detected: %d\n", inconsistentReads)

	if inconsistentReads > 0 {
		fmt.Printf("❌ RACE CONDITION DETECTED! Readers saw inconsistent state\n")
	} else {
		fmt.Printf("✓ No inconsistent reads (got lucky, or not enough contention)\n")
	}
}