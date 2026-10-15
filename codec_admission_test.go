package hearsay

import (
	"bytes"
	"testing"

	"github.com/cirocosta/hearsay/internal/cbor"
)

func TestPayloadAdmissionDoesNotImposeScannerDefaultCaps(t *testing.T) {
	codec := Codec[[]byte]{
		Encode:        func(p []byte) ([]byte, error) { return p, nil },
		DecodeBounded: func(p []byte, _ uint64) ([]byte, error) { return p, nil },
		Equal:         bytes.Equal,
	}
	t.Run("more than one million items", func(t *testing.T) {
		const count = 1_000_001
		encoded := cbor.AppendHead(nil, cbor.MajorArray, count)
		encoded = append(encoded, make([]byte, count)...)
		owned, err := codec.encode(encoded)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(owned, encoded) {
			t.Fatal("admission changed payload")
		}
	})
	t.Run("more than 64 MiB", func(t *testing.T) {
		const length = (64 << 20) + 1
		encoded := cbor.AppendHead(nil, cbor.MajorBytes, length)
		encoded = append(encoded, make([]byte, length)...)
		owned, err := codec.encode(encoded)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(owned, encoded) {
			t.Fatal("admission changed payload")
		}
	})
}
