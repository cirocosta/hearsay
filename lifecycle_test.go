package hearsay

import (
	"bytes"
	"testing"
)

func TestSeedRejectsReservedBootstrapNetwork(t *testing.T) {
	if _, err := SeedFrom(bytes.NewReader(make([]byte, 32)), BytesCodec()); err == nil {
		t.Fatal("seed admitted reserved bootstrap network")
	}
	entropy := make([]byte, 32)
	entropy[31] = 1
	peer, err := SeedFrom(bytes.NewReader(entropy), BytesCodec())
	if err != nil {
		t.Fatal(err)
	}
	if peer.Network() == (Network{}) {
		t.Fatal("reserved network after retry")
	}
	if err := peer.Close(); err != nil {
		t.Fatal(err)
	}
}
