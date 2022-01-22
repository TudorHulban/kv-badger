package badger

import (
	"github.com/TudorHulban/kv"

	badger "github.com/dgraph-io/badger/v2"
)

// GetVByK fetches value from store based on passed key.
func (s BStore) GetVByK(key []byte) ([]byte, error) {
	var res []byte

	errView := s.Store.View(func(txn *badger.Txn) error {
		value, errGet := txn.Get([]byte(key))
		if errGet != nil {
			return errGet
		}

		go s.logger.Debugf("size: %v, expires: %v", value.EstimatedSize(), value.ExpiresAt())

		return value.Value(func(itemVals []byte) error {
			res = append(res, itemVals...)
			return nil
		})
	})

	return res, errView
}

// GetAnyByK fetches value and injects it into passed pointer type.
func (s BStore) GetAnyByK(key []byte, decodeInTo interface{}) error {
	value, errGet := s.GetVByK(key)
	if errGet != nil {
		return errGet
	}

	return Decoder([]byte(value), decodeInTo)
}

func (s BStore) GetKVByPrefix(keyPrefix []byte) ([]kv.KV, error) {
	var result []kv.KV

	errView := s.Store.View(func(txn *badger.Txn) error {
		options := badger.DefaultIteratorOptions
		options.PrefetchSize = 10

		iterator := txn.NewIterator(options)
		defer iterator.Close()

		var errItem error

		for iterator.Seek(keyPrefix); iterator.ValidForPrefix(keyPrefix); iterator.Next() {
			item := iterator.Item()
			k := item.Key()

			errItem = item.Value(func(itemValue []byte) error {
				go s.logger.Debugf("key=%s, value=%s", k, itemValue)

				result = append(result, kv.KV{
					Key:   k,
					Value: itemValue,
				})
				return nil
			})

			// early exit if any error
			if errItem != nil {
				return errItem
			}
		}

		return nil
	})

	return result, errView
}
