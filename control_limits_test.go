package hearsay

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/cirocosta/hearsay/internal/cbor"
	"github.com/cirocosta/hearsay/internal/protocol"
)

func TestControlBodiesGrowFromActualReceiptWithout64MiBCap(t *testing.T) {
	const declared = 65 << 20
	t.Run("greeting", func(t *testing.T) {
		item := cbor.AppendHead(nil, cbor.MajorTag, cbor.TagEmbeddedItem)
		item = cbor.AppendHead(item, cbor.MajorBytes, declared)
		_, err := readGreeting(bytes.NewReader(item))
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("declared greeting rejected before body receipt: %v", err)
		}
	})
	t.Run("identity", func(t *testing.T) {
		item := cbor.AppendHead(nil, cbor.MajorTag, protocol.PartyTag)
		item = cbor.AppendHead(item, cbor.MajorBytes, declared)
		_, err := readParty(bytes.NewReader(item))
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("declared identity rejected before body receipt: %v", err)
		}
	})
}
