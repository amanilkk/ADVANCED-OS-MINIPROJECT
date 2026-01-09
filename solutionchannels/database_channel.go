package solutionchannels

import (
	"fmt"
	"time"
)

//
// ----------------------------
// DATA STRUCTURES
// ----------------------------
//

// Record représente une entrée de la base de données.
// Chaque record contient :
// - une clé
// - une valeur entière
// - une version (utile pour détecter les mises à jour)
// - un timestamp de la dernière modification
type Record struct {
	Key       string
	Value     int
	Version   int
	UpdatedAt time.Time
}

// Transaction représente une transaction logique.
// Elle sert principalement à identifier une transaction
// et à mesurer sa durée.
type Transaction struct {
	ID        int
	StartTime time.Time
}

// Stats permet de collecter des statistiques globales
// sur l'utilisation de la base de données.
type Stats struct {
	TotalReads   int
	TotalWrites  int
	TotalUpdates int
}

// Database est la structure principale de la base.
// Elle implémente une synchronisation "coarse-grained".
type Database struct {
	// lockChan est un canal utilisé comme verrou exclusif.
	// Sa capacité est 1 → une seule transaction peut l’occuper.
	lockChan chan struct{}

	// records contient les données de la base (clé → record).
	records map[string]*Record

	// txCounter sert à attribuer des identifiants uniques aux transactions.
	txCounter int

	// stats stocke les statistiques d'accès.
	stats Stats
}

//
// ----------------------------
// CONSTRUCTOR
// ----------------------------
//

// NewDatabase initialise la base de données.
// Le canal lockChan est pré-rempli pour représenter
// un verrou initialement libre.
func NewDatabase() *Database {
	db := &Database{
		lockChan: make(chan struct{}, 1),
		records:  make(map[string]*Record),
	}

	// Insertion d’un token dans le canal :
	// cela signifie que la base est initialement "déverrouillée".
	db.lockChan <- struct{}{}
	return db
}

//
// ----------------------------
// TRANSACTION MANAGEMENT
// ----------------------------
//

// BeginTransaction démarre une transaction.
// Elle bloque tant que le verrou n’est pas disponible.
// Cela garantit qu’une seule transaction peut être active à la fois.
func (db *Database) BeginTransaction() *Transaction {
	<-db.lockChan // acquisition du verrou (exclusivité totale)

	db.txCounter++
	return &Transaction{
		ID:        db.txCounter,
		StartTime: time.Now(),
	}
}

// Commit termine une transaction.
// Il libère le verrou, permettant à une autre transaction de commencer.
func (db *Database) Commit(tx *Transaction) {
	db.lockChan <- struct{}{} // libération du verrou
}

// Abort annule une transaction.
// Dans cette implémentation simple, Abort équivaut à Commit,
// car il n’y a pas de journal ou rollback.
func (db *Database) Abort(tx *Transaction) {
	db.lockChan <- struct{}{}
}

//
// ----------------------------
// CRUD OPERATIONS
// ----------------------------
//

// Read lit une valeur associée à une clé.
// Comme la transaction détient le verrou, la lecture est isolée
// et ne peut pas voir d’état intermédiaire.
func (db *Database) Read(tx *Transaction, key string) (int, bool) {
	db.stats.TotalReads++

	rec, ok := db.records[key]
	if !ok {
		return 0, false
	}
	return rec.Value, true
}

// Write écrit ou remplace complètement la valeur d’une clé.
// Si la clé existe, la version est incrémentée.
// Sinon, un nouveau record est créé.
func (db *Database) Write(tx *Transaction, key string, value int) {
	db.stats.TotalWrites++

	rec, ok := db.records[key]
	if ok {
		rec.Value = value
		rec.Version++
		rec.UpdatedAt = time.Now()
	} else {
		db.records[key] = &Record{
			Key:       key,
			Value:     value,
			Version:   1,
			UpdatedAt: time.Now(),
		}
	}
}

// Update applique une modification incrémentale (delta).
// Elle est typiquement utilisée pour les compteurs.
// Retourne false si la clé n’existe pas.
func (db *Database) Update(tx *Transaction, key string, delta int) bool {
	db.stats.TotalUpdates++

	rec, ok := db.records[key]
	if !ok {
		return false
	}

	rec.Value += delta
	rec.Version++
	rec.UpdatedAt = time.Now()
	return true
}

// Delete supprime un record de la base.
// Retourne false si la clé n’existe pas.
func (db *Database) Delete(tx *Transaction, key string) bool {
	if _, ok := db.records[key]; !ok {
		return false
	}
	delete(db.records, key)
	return true
}

//
// ----------------------------
// UTILITIES
// ----------------------------
//

// GetStats retourne une copie des statistiques globales.
func (db *Database) GetStats() Stats {
	return db.stats
}

// PrintRecords affiche l’état courant de la base.
// Cette fonction suppose qu’aucune autre transaction
// n’est en cours d’exécution.
func (db *Database) PrintRecords() {
	fmt.Println("=== Records ===")
	for k, r := range db.records {
		fmt.Printf("%s = %d (v%d)\n", k, r.Value, r.Version)
	}
}
