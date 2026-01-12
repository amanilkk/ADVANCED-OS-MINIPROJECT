package solutionchannels

import (
	"fmt"
	"time"
)

//
// ============================
// INTERNAL OPERATION MODEL
// ============================
//

type operationType int

const (
	opBegin operationType = iota
	opCommit
	opRead
	opWrite
	opUpdate
	opDelete
	opStats
)

type operation struct {
	opType     operationType
	tx         *Transaction
	key        string
	value      int
	delta      int
	resultChan chan any
}

//
// ============================
// DATA STRUCTURES
// ============================
//

type Record struct {
	Key       string
	Value     int
	Version   int
	UpdatedAt time.Time
}

type Transaction struct {
	ID        int
	StartTime time.Time
}

type Stats struct {
	TotalReads   int
	TotalWrites  int
	TotalUpdates int
}

//
// ============================
// DATABASE
// ============================
//

type Database struct {
	opChan chan operation

	records map[string]*Record
	stats   Stats

	txCounter int
	activeTx  *Transaction

	waitingBegins []chan any 
}

//
// ============================
// CONSTRUCTOR
// ============================
//

func NewDatabase() *Database {
	db := &Database{
		opChan:  make(chan operation),
		records: make(map[string]*Record),
	}
	go db.run()
	return db
}

//
// ============================
// EVENT LOOP (SAFE, NO DEADLOCK)
// ============================
//

func (db *Database) run() {
	for op := range db.opChan {

		switch op.opType {

		// ----------------------------
		// BEGIN TRANSACTION
		// ----------------------------

		case opBegin:
			if db.activeTx == nil {
				db.startTransaction(op.resultChan)
			} else {
				db.waitingBegins = append(db.waitingBegins, op.resultChan)
			}

		// ----------------------------
		// COMMIT
		// ----------------------------

		case opCommit:
			if db.activeTx != nil && db.activeTx.ID == op.tx.ID {
				db.activeTx = nil

				// start next waiting transaction (FIFO)
				if len(db.waitingBegins) > 0 {
					next := db.waitingBegins[0]
					db.waitingBegins = db.waitingBegins[1:]
					db.startTransaction(next)
				}
			}
			op.resultChan <- true

		// ----------------------------
		// DATA OPERATIONS
		// ----------------------------

		default:
			// enforce transaction isolation
			if db.activeTx == nil || db.activeTx.ID != op.tx.ID {
				// invalid access → ignore safely
				continue
			}
			db.execute(op)
		}
	}
}

func (db *Database) startTransaction(ch chan any) {
	db.txCounter++
	tx := &Transaction{
		ID:        db.txCounter,
		StartTime: time.Now(),
	}
	db.activeTx = tx
	ch <- tx
}

//
// ============================
// OPERATION EXECUTION
// ============================
//

func (db *Database) execute(op operation) {
	switch op.opType {

	case opRead:
		db.stats.TotalReads++
		rec, ok := db.records[op.key]
		if !ok {
			op.resultChan <- struct {
				val int
				ok  bool
			}{0, false}
		} else {
			op.resultChan <- struct {
				val int
				ok  bool
			}{rec.Value, true}
		}

	case opWrite:
		db.stats.TotalWrites++
		rec, ok := db.records[op.key]
		if ok {
			rec.Value = op.value
			rec.Version++
			rec.UpdatedAt = time.Now()
		} else {
			db.records[op.key] = &Record{
				Key:       op.key,
				Value:     op.value,
				Version:   1,
				UpdatedAt: time.Now(),
			}
		}
		op.resultChan <- true

	case opUpdate:
		db.stats.TotalUpdates++
		rec, ok := db.records[op.key]
		if !ok {
			op.resultChan <- false
			return
		}
		rec.Value += op.delta
		rec.Version++
		rec.UpdatedAt = time.Now()
		op.resultChan <- true

	case opDelete:
		_, ok := db.records[op.key]
		if ok {
			delete(db.records, op.key)
		}
		op.resultChan <- ok

	case opStats:
		op.resultChan <- db.stats
	}
}

//
// ============================
// TRANSACTION API
// ============================
//

func (db *Database) BeginTransaction() *Transaction {
	ch := make(chan any)
	db.opChan <- operation{
		opType:     opBegin,
		resultChan: ch,
	}
	return (<-ch).(*Transaction)
}

func (db *Database) Commit(tx *Transaction) {
	ch := make(chan any)
	db.opChan <- operation{
		opType:     opCommit,
		tx:         tx,
		resultChan: ch,
	}
	<-ch
}

func (db *Database) Abort(tx *Transaction) {
	db.Commit(tx)
}

//
// ============================
// CRUD OPERATIONS
// ============================
//

func (db *Database) Read(tx *Transaction, key string) (int, bool) {
	ch := make(chan any)
	db.opChan <- operation{
		opType:     opRead,
		tx:         tx,
		key:        key,
		resultChan: ch,
	}
	res := (<-ch).(struct {
		val int
		ok  bool
	})
	return res.val, res.ok
}

func (db *Database) Write(tx *Transaction, key string, value int) {
	ch := make(chan any)
	db.opChan <- operation{
		opType:     opWrite,
		tx:         tx,
		key:        key,
		value:      value,
		resultChan: ch,
	}
	<-ch
}

func (db *Database) Update(tx *Transaction, key string, delta int) bool {
	ch := make(chan any)
	db.opChan <- operation{
		opType:     opUpdate,
		tx:         tx,
		key:        key,
		delta:      delta,
		resultChan: ch,
	}
	return (<-ch).(bool)
}

func (db *Database) Delete(tx *Transaction, key string) bool {
	ch := make(chan any)
	db.opChan <- operation{
		opType:     opDelete,
		tx:         tx,
		key:        key,
		resultChan: ch,
	}
	return (<-ch).(bool)
}

//
// ============================
// UTILITIES
// ============================
//

func (db *Database) GetStats() Stats {
	ch := make(chan any)
	db.opChan <- operation{
		opType:     opStats,
		resultChan: ch,
	}
	return (<-ch).(Stats)
}

func (db *Database) PrintRecords() {
	tx := db.BeginTransaction()
	fmt.Println("=== Records ===")
	for k, r := range db.records {
		fmt.Printf("%s = %d (v%d)\n", k, r.Value, r.Version)
	}
	db.Commit(tx)
}
