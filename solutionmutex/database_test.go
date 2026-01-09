package solutionmutex

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// ============================================================================
// CORRECTNESS TESTS
// These tests verify that the database maintains data integrity under
// concurrent access. With proper synchronization, all tests should pass.
// With the unsynchronized version, these tests will likely fail or show
// race conditions when run with: go test -race
// ============================================================================

// TestCounterIncrement tests the counter increment scenario
// This is the classic "lost update" problem
func TestCounterIncrement(t *testing.T) {
	db := NewDatabase()

	// Initialize counter
	tx := db.BeginTransaction()
	db.Write(tx, "counter", 0)
	db.Commit(tx)

	numClients := 10
	incrementsPerClient := 100
	expectedFinal := numClients * incrementsPerClient

	var wg sync.WaitGroup

	// Each client increments the counter
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < incrementsPerClient; j++ {
				tx := db.BeginTransaction()
				db.Update(tx, "counter", 1)
				db.Commit(tx)
			}
		}()
	}

	wg.Wait()

	// Verify final value
	tx = db.BeginTransaction()
	finalValue, exists := db.Read(tx, "counter")
	db.Commit(tx)

	if !exists {
		t.Fatalf("counter key not found")
	}

	if finalValue != expectedFinal {
		t.Errorf("expected counter=%d, got %d (lost %d updates)",
			expectedFinal, finalValue, expectedFinal-finalValue)
	}
}

// TestBankTransfer tests the bank transfer scenario
// This verifies that the total balance is preserved across transfers
func TestBankTransfer(t *testing.T) {
	db := NewDatabase()

	// Initialize accounts
	tx := db.BeginTransaction()
	db.Write(tx, "account_A", 1000)
	db.Write(tx, "account_B", 1000)
	db.Commit(tx)

	initialTotal := 2000
	numClients := 5
	transfersPerClient := 50

	var wg sync.WaitGroup

	// Each client transfers money
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()

			for j := 0; j < transfersPerClient; j++ {
				amount := 10 // Fixed amount for deterministic testing

				tx := db.BeginTransaction()

				// Read both accounts
				balanceA, _ := db.Read(tx, "account_A")
				balanceB, _ := db.Read(tx, "account_B")

				// Transfer from A to B
				db.Write(tx, "account_A", balanceA-amount)
				db.Write(tx, "account_B", balanceB+amount)

				db.Commit(tx)
			}
		}(i)
	}

	wg.Wait()

	// Verify total is preserved
	tx = db.BeginTransaction()
	finalA, _ := db.Read(tx, "account_A")
	finalB, _ := db.Read(tx, "account_B")
	db.Commit(tx)

	finalTotal := finalA + finalB

	if finalTotal != initialTotal {
		t.Errorf("total not preserved! expected=%d, got=%d (lost %d)",
			initialTotal, finalTotal, initialTotal-finalTotal)
	}
}

// TestConcurrentReadWrite tests concurrent reads and writes
// This verifies isolation - readers should not see partial updates
func TestConcurrentReadWrite(t *testing.T) {
	db := NewDatabase()

	// Initialize data - both values should always be equal
	tx := db.BeginTransaction()
	db.Write(tx, "data_1", 100)
	db.Write(tx, "data_2", 100)
	db.Commit(tx)

	stopChan := make(chan bool)
	var wg sync.WaitGroup

	inconsistentReads := 0
	var inconsistentMutex sync.Mutex

	numReaders := 5
	numWriters := 3

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

	// Start writers
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			value := 100

			for {
				select {
				case <-stopChan:
					return
				default:
					tx := db.BeginTransaction()
					value++

					// Write same value to both
					db.Write(tx, "data_1", value)
					db.Write(tx, "data_2", value)

					db.Commit(tx)
					time.Sleep(time.Microsecond * 100)
				}
			}
		}(i)
	}

	// Run for a short duration
	time.Sleep(100 * time.Millisecond)
	close(stopChan)
	wg.Wait()

	// With proper synchronization, there should be no inconsistent reads
	if inconsistentReads > 0 {
		t.Errorf("detected %d inconsistent reads (synchronization may be insufficient)",
			inconsistentReads)
	}
}

// TestBasicOperations tests basic CRUD operations
func TestBasicOperations(t *testing.T) {
	db := NewDatabase()

	// Test Write
	tx := db.BeginTransaction()
	db.Write(tx, "key1", 42)
	db.Commit(tx)

	// Test Read
	tx = db.BeginTransaction()
	value, exists := db.Read(tx, "key1")
	db.Commit(tx)

	if !exists {
		t.Fatalf("key1 should exist")
	}
	if value != 42 {
		t.Errorf("expected value=42, got %d", value)
	}

	// Test Update
	tx = db.BeginTransaction()
	success := db.Update(tx, "key1", 8)
	db.Commit(tx)

	if !success {
		t.Fatalf("update should succeed")
	}

	tx = db.BeginTransaction()
	value, _ = db.Read(tx, "key1")
	db.Commit(tx)

	if value != 50 {
		t.Errorf("expected value=50 after update, got %d", value)
	}

	// Test Delete
	tx = db.BeginTransaction()
	success = db.Delete(tx, "key1")
	db.Commit(tx)

	if !success {
		t.Fatalf("delete should succeed")
	}

	tx = db.BeginTransaction()
	_, exists = db.Read(tx, "key1")
	db.Commit(tx)

	if exists {
		t.Errorf("key1 should not exist after delete")
	}
}

// TestStressTest runs a high-concurrency stress test
func TestStressTest(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	db := NewDatabase()

	// Initialize multiple counters
	for i := 0; i < 10; i++ {
		tx := db.BeginTransaction()
		db.Write(tx, fmt.Sprintf("counter_%d", i), 0)
		db.Commit(tx)
	}

	numClients := 20
	opsPerClient := 100

	var wg sync.WaitGroup

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()

			for j := 0; j < opsPerClient; j++ {
				key := fmt.Sprintf("counter_%d", j%10)

				tx := db.BeginTransaction()
				db.Update(tx, key, 1)
				db.Commit(tx)
			}
		}(i)
	}

	wg.Wait()

	// Verify all counters
	expectedPerCounter := (numClients * opsPerClient) / 10

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("counter_%d", i)
		tx := db.BeginTransaction()
		value, exists := db.Read(tx, key)
		db.Commit(tx)

		if !exists {
			t.Errorf("%s should exist", key)
			continue
		}

		if value != expectedPerCounter {
			t.Errorf("%s expected=%d, got=%d",
				key, expectedPerCounter, value)
		}
	}
}

// ============================================================================
// BENCHMARK TESTS
// These benchmarks measure performance of the database operations.
// Run with: go test -bench=. -benchmem
// ============================================================================

// BenchmarkWrites benchmarks write performance
func BenchmarkWrites(b *testing.B) {
	db := NewDatabase()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			tx := db.BeginTransaction()
			db.Write(tx, fmt.Sprintf("key_%d", i%100), i)
			db.Commit(tx)
			i++
		}
	})
}

// BenchmarkReads benchmarks read performance
func BenchmarkReads(b *testing.B) {
	db := NewDatabase()

	// Pre-populate database
	for i := 0; i < 100; i++ {
		tx := db.BeginTransaction()
		db.Write(tx, fmt.Sprintf("key_%d", i), i)
		db.Commit(tx)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			tx := db.BeginTransaction()
			db.Read(tx, fmt.Sprintf("key_%d", i%100))
			db.Commit(tx)
			i++
		}
	})
}

// BenchmarkMixed benchmarks mixed read/write workload
func BenchmarkMixed(b *testing.B) {
	db := NewDatabase()

	// Pre-populate database
	for i := 0; i < 100; i++ {
		tx := db.BeginTransaction()
		db.Write(tx, fmt.Sprintf("key_%d", i), i)
		db.Commit(tx)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			tx := db.BeginTransaction()
			if i%10 == 0 {
				// 10% writes
				db.Write(tx, fmt.Sprintf("key_%d", i%100), i)
			} else {
				// 90% reads
				db.Read(tx, fmt.Sprintf("key_%d", i%100))
			}
			db.Commit(tx)
			i++
		}
	})
}

// BenchmarkCounterIncrement benchmarks the counter increment scenario
func BenchmarkCounterIncrement(b *testing.B) {
	db := NewDatabase()

	// Initialize counter
	tx := db.BeginTransaction()
	db.Write(tx, "counter", 0)
	db.Commit(tx)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			tx := db.BeginTransaction()
			db.Update(tx, "counter", 1)
			db.Commit(tx)
		}
	})
}

// BenchmarkContentionHigh benchmarks performance under high contention
func BenchmarkContentionHigh(b *testing.B) {
	db := NewDatabase()

	// Initialize a single key (high contention)
	tx := db.BeginTransaction()
	db.Write(tx, "hotkey", 0)
	db.Commit(tx)

	b.ResetTimer()

	var wg sync.WaitGroup

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			wg.Add(1)
			go func() {
				defer wg.Done()
				tx := db.BeginTransaction()
				db.Update(tx, "hotkey", 1)
				db.Commit(tx)
			}()
		}
	})

	wg.Wait()
}
