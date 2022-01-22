package badger

import (
	"os"
	"sync"
	"testing"

	"github.com/TudorHulban/kv"

	"github.com/TudorHulban/log"
	"github.com/stretchr/testify/assert"
)

// Target of test:
// a. that get by prefix returns correct elements in slice.
func TestGetByPrefix(t *testing.T) {
	l := log.NewLogger(log.DEBUG, os.Stderr, true)

	inmemStore, err := NewBStoreInMem(l)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, inmemStore.Close())
	}()

	kPrefix := "prefix-"

	// inserting first element.
	kv1 := kv.KV{
		Key:   []byte(kPrefix + "x1"),
		Value: []byte("y1"),
	}
	assert.NoError(t, inmemStore.Set(kv1))

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		kv2 := kv.KV{
			Key:   []byte(kPrefix + "x2"),
			Value: []byte("y2"),
		}

		assert.NoError(t, inmemStore.Set(kv2))

		wg.Done()
	}()

	go func() {
		kv3 := kv.KV{
			Key:   []byte(kPrefix + "x3"),
			Value: []byte("y3"),
		}

		assert.NoError(t, inmemStore.Set(kv3))

		wg.Done()
	}()

	wg.Wait()

	v, errGet := inmemStore.GetKVByPrefix([]byte(kPrefix))
	assert.NoError(t, errGet)
	assert.Equal(t, len(v), 3) // a.
	assert.Contains(t, v, kv1) // a.

	vBadPrefix, errBadPrefix := inmemStore.GetKVByPrefix([]byte("xxxxxxxxxx"))
	assert.NoError(t, errBadPrefix)
	assert.Equal(t, 0, len(vBadPrefix)) // a.
}
