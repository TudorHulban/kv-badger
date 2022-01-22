package badger

import (
	"os"
	"testing"

	"github.com/TudorHulban/kv"

	"github.com/TudorHulban/log"
	"github.com/stretchr/testify/assert"
)

func TestDelete(t *testing.T) {
	l := log.NewLogger(log.DEBUG, os.Stderr, true)

	inMemoryStore, err := NewBStoreInMem(l)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, inMemoryStore.Close())
	}()

	kv := kv.KV{
		Key:   []byte("x"),
		Value: []byte("y"),
	}

	// test insert
	assert.NoError(t, inMemoryStore.Set(kv))

	v0, errGet := inMemoryStore.GetVByK(kv.Key)
	assert.NoError(t, errGet)
	assert.Equal(t, v0, []byte(kv.Value))

	// now delete the KV
	assert.NoError(t, inMemoryStore.DeleteKVByK(kv.Key))

	v1, errGet := inMemoryStore.GetVByK(kv.Key)
	assert.NoError(t, errGet)
	assert.Nil(t, v1)
}
