package badger

import (
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/TudorHulban/kv"

	"github.com/TudorHulban/log"
	"github.com/stretchr/testify/assert"
)

func TestSet(t *testing.T) {
	l := log.NewLogger(log.DEBUG, os.Stderr, true)

	inMemoryStore, err := NewBStoreInMem(l)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, inMemoryStore.Close())
	}()

	kPrefix := "prefix-"
	kv := kv.KV{
		Key:   []byte(kPrefix + "x"),
		Value: []byte("y"),
	}

	// test insert
	assert.NoError(t, inMemoryStore.Set(kv))

	// test update
	kv.Value = []byte("z")
	assert.NoError(t, inMemoryStore.Set(kv))

	v, errGet := inMemoryStore.GetVByK(kv.Key)
	assert.NoError(t, errGet)
	assert.Equal(t, v, []byte(kv.Value))
}

func TestClose(t *testing.T) {
	inMemoryStore, err := NewBStoreInMemNoLogging()
	assert.NoError(t, err)
	assert.NoError(t, inMemoryStore.Close())

	// test insert on closed store.
	kv := kv.KV{
		Key:   []byte("x"),
		Value: []byte("y"),
	}
	assert.Error(t, inMemoryStore.Set(kv))
}

func TestTTL(t *testing.T) {
	l := log.NewLogger(log.DEBUG, os.Stderr, true)

	inmemStore, err := NewBStoreInMem(l)
	assert.Nil(t, err)
	defer func() {
		assert.Nil(t, inmemStore.Close())
	}()

	kPrefix := "prefix-"
	kv := kv.KV{
		Key:   []byte(kPrefix + "x"),
		Value: []byte("y"),
	}
	ttlSeconds := 1

	assert.Nil(t, inmemStore.SetTTL(kv, uint(ttlSeconds)))

	time.Sleep(time.Duration(ttlSeconds+1) * time.Second)
	_, errGet := inmemStore.GetVByK(kv.Key)
	assert.Error(t, errGet)
}

// BenchmarkSet-4   	   19934	     59591 ns/op	    1367 B/op	      34 allocs/op
func BenchmarkSet(b *testing.B) {
	inmemStore, _ := NewBStoreInMemNoLogging()
	defer func() {
		inmemStore.Close()
	}()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		inmemStore.Set(kv.KV{
			Key:   []byte(strconv.Itoa(i)),
			Value: []byte("x"),
		})
	}
}
