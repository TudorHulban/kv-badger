package badger

import (
	"kv"
	"os"
	"testing"

	"github.com/TudorHulban/log"
	"github.com/stretchr/testify/assert"
)

func TestDelete(t *testing.T) {
	l := log.New(log.DEBUG, os.Stderr, true)

	inmemStore, err := NewBStoreInMem(l)
	assert.Nil(t, err)
	defer func() {
		assert.Nil(t, inmemStore.Close())
	}()

	kv := kv.KV{[]byte("x"), []byte("y")}

	// test insert
	assert.Nil(t, inmemStore.Set(kv))

	v0, errGet := inmemStore.GetVByK(kv.Key)
	assert.Nil(t, errGet)
	assert.Equal(t, v0, []byte(kv.Value))

	// now delete the KV
	assert.Nil(t, inmemStore.DeleteKVByK(kv.Key))
	v1, errGet := inmemStore.GetVByK(kv.Key)
	assert.Error(t, errGet)
	assert.Nil(t, v1)
}
