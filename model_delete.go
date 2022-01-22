package badger

import badger "github.com/dgraph-io/badger/v2"

// DeleteKVByK Deletes KV by key.
func (s BStore) DeleteKVByK(key []byte) error {
	return s.Store.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})
}
