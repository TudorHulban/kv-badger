package badger

import (
	"kv"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/TudorHulban/log"
	"github.com/stretchr/testify/assert"
)

func TestSet(t *testing.T) {
	l := log.New(log.DEBUG, os.Stderr, true)

	inmemStore, err := NewBStoreInMem(l)
	assert.Nil(t, err)
	defer func() {
		assert.Nil(t, inmemStore.Close())
	}()

	kPrefix := "prefix-"
	kv := kv.KV{[]byte(kPrefix + "x"), []byte("y")}

	// test insert
	assert.Nil(t, inmemStore.Set(kv))

	// test update
	kv.Value = []byte("z")
	assert.Nil(t, inmemStore.Set(kv))

	v, errGet := inmemStore.GetVByK(kv.Key)
	assert.Nil(t, errGet)
	assert.Equal(t, v, []byte(kv.Value))
}

func TestClose(t *testing.T) {
	inmemStore, err := NewBStoreInMemNoLogging()
	assert.Nil(t, err)
	assert.Nil(t, inmemStore.Close())

	// test insert on closed store.
	kv := kv.KV{[]byte("x"), []byte("y")}
	errSet := inmemStore.Set(kv)
	assert.Error(t, errSet)
}

func TestTTL(t *testing.T) {
	l := log.New(log.DEBUG, os.Stderr, true)

	inmemStore, err := NewBStoreInMem(l)
	assert.Nil(t, err)
	defer func() {
		assert.Nil(t, inmemStore.Close())
	}()

	kPrefix := "prefix-"
	kv := kv.KV{[]byte(kPrefix + "x"), []byte("y")}
	ttlSeconds := 1

	errSet := inmemStore.SetTTL(kv, uint(ttlSeconds))
	assert.Nil(t, errSet)

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
			[]byte(strconv.Itoa(i)),
			[]byte("x"),
		})
	}
}
