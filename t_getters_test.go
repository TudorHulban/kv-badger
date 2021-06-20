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
	assert.Nil(t, err)
	defer func() {
		assert.Nil(t, inmemStore.Close())
	}()

	kPrefix := "prefix-"

	// inserting first element.
	kv1 := kv.KV{[]byte(kPrefix + "x1"), []byte("y1")}
	errSet := inmemStore.Set(kv1)
	assert.Nil(t, errSet)

	var wg sync.WaitGroup

	wg.Add(2)

	go func() {
		kv2 := kv.KV{[]byte(kPrefix + "x2"), []byte("y2")}
		errSet := inmemStore.Set(kv2)
		assert.Nil(t, errSet)

		wg.Done()
	}()

	go func() {
		kv3 := kv.KV{[]byte(kPrefix + "x3"), []byte("y3")}
		errSet := inmemStore.Set(kv3)
		assert.Nil(t, errSet)

		wg.Done()
	}()

	wg.Wait()

	v, errGet := inmemStore.GetKVByPrefix([]byte(kPrefix))
	assert.Nil(t, errGet)
	assert.Equal(t, len(v), 3) // a.
	assert.Contains(t, v, kv1) // a.

	vBadPrefix, errBadPrefix := inmemStore.GetKVByPrefix([]byte("xxxxxxxxxx"))
	assert.Nil(t, errBadPrefix)
	assert.Equal(t, 0, len(vBadPrefix)) // a.
}
