package badger

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type tstruct struct {
	F1 int
	F2 string
}

func TestGobers(t *testing.T) {
	v := tstruct{
		F1: 1,
		F2: "a",
	}

	// testing encoder
	resEnc, errEncode := Encoder(v)
	assert.Nil(t, errEncode)
	assert.NotNil(t, resEnc)

	// testing decoder
	p := new(tstruct)
	errDecode := Decoder(resEnc, p)
	assert.Nil(t, errDecode)
	assert.Equal(t, v, *p)
}
