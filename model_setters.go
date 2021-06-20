package badger

import (
	"time"

	"github.com/TudorHulban/kv"

	badger "github.com/dgraph-io/badger/v2"
)

// Set sets or updates key in store.
func (s BStore) Set(theKV kv.KV) error {
	return s.Store.Update(func(txn *badger.Txn) error {
		return txn.Set(theKV.Key, theKV.Value)
	})
}

// SetAny sets or updates key in store.
func (s BStore) SetAny(key []byte, value interface{}) error {
	v, errEncode := anyEncoder(value)
	if errEncode != nil {
		return errEncode
	}

	return s.Set(kv.KV{
		Key:   key,
		Value: v,
	})
}

// SetAnyTTL sets or updates key in store.
func (s BStore) SetAnyTTL(key []byte, value interface{}, ttlSecs uint) error {
	v, errEncode := anyEncoder(value)
	if errEncode != nil {
		return errEncode
	}

	return s.SetTTL(kv.KV{
		Key:   key,
		Value: v,
	}, ttlSecs)
}

// SetTTL can be used for inserts and updates. Time To Live in seconds.
func (s BStore) SetTTL(theKV kv.KV, ttlSecs uint) error {
	return s.Store.Update(func(txn *badger.Txn) error {
		entry := badger.NewEntry(theKV.Key, theKV.Value).WithTTL(time.Second * time.Duration(ttlSecs))
		return txn.SetEntry(entry)
	})
}
