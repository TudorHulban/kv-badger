package badger

// Different constructors for disk sync options for better usability.
// Passing logger as pointer as we might not want more than one logger.

import (
	"github.com/TudorHulban/log"
	badger "github.com/dgraph-io/badger/v2"
	"github.com/pkg/errors"
)

// BStore Concentrates information defining a KV store.
type BStore struct {
	logger *log.Logger // logger needed only for package logging
	Store  *badger.DB
}

// NewBStoreDiskWSyncWrites returns a type containing a store that satisfies store interface.
func NewBStoreDiskWSyncWrites(dbFilePath string, l *log.Logger) (*BStore, error) {
	db, errOpen := badger.Open(badger.DefaultOptions(dbFilePath))
	if errOpen != nil {
		return nil, errors.WithMessagef(errOpen, "could not open passed file %s in NewBStoreDiskWSyncWrites", dbFilePath)
	}

	return &BStore{
		logger: l,
		Store:  db,
	}, nil
}

// NewBStoreDisk returns a type containing a store that satisfies store interface.
// No sync writes.
func NewBStoreDisk(dbFilePath string, l *log.Logger) (*BStore, error) {
	db, errOpen := badger.Open(badger.DefaultOptions(dbFilePath).WithSyncWrites(false))
	if errOpen != nil {
		return nil, errors.WithMessagef(errOpen, "could not open passed file %s in NewBStoreDisk", dbFilePath)
	}

	return &BStore{
		logger: l,
		Store:  db,
	}, nil
}

// NewBStoreInMem Creates in memory Badger DB.
func NewBStoreInMem(extLogger *log.Logger) (*BStore, error) {
	db, errOpen := badger.Open(badger.DefaultOptions("").WithInMemory(true))
	if errOpen != nil {
		return nil, errors.WithMessage(errOpen, "when creating in memory store")
	}

	return &BStore{
		logger: extLogger,
		Store:  db,
	}, nil
}

// NewBStoreInMemNoLogging Creates in memory Badger DB.
// No protection for writing to nil logger field.
func NewBStoreInMemNoLogging() (*BStore, error) {
	options := badger.DefaultOptions("").WithInMemory(true).WithLogger(nil)

	db, errOpen := badger.Open(options)
	if errOpen != nil {
		return nil, errors.WithMessage(errOpen, "when creating in memory store")
	}

	return &BStore{
		Store: db,
	}, nil
}

// Close closes the store.
func (s BStore) Close() error {
	return s.Store.Close()
}
