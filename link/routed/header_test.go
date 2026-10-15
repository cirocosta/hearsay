package routed

import (
	"bytes"
	"encoding/hex"
	"io"
	"testing"
)

func TestHeaderUpstreamLayout(t *testing.T) {
	var token Token
	for i := range token {
		token[i] = byte(i)
	}
	var wire bytes.Buffer
	if err := writeHeader(&wire, kindStream, token, nil); err != nil {
		t.Fatal(err)
	}
	const stream = "524f555445444c494e4b0102000102030405060708090a0b0c0d0e0f"
	if got := hex.EncodeToString(wire.Bytes()); got != stream {
		t.Fatalf("stream header %s", got)
	}
	h, err := readHeader(&wire)
	if err != nil {
		t.Fatal(err)
	}
	if h.kind != kindStream || h.token != token || len(h.peer) != 0 {
		t.Fatal(h)
	}
	if err = writeHeader(&wire, kindLink, token, []byte{9, 8}); err != nil {
		t.Fatal(err)
	}
	const establishment = "524f555445444c494e4b0101000102030405060708090a0b0c0d0e0f020908"
	if got := hex.EncodeToString(wire.Bytes()); got != establishment {
		t.Fatalf("link header %s", got)
	}
	h, err = readHeader(&wire)
	if err != nil {
		t.Fatal(err)
	}
	if h.kind != kindLink || h.token != token || !bytes.Equal(h.peer, []byte{9, 8}) {
		t.Fatal(h)
	}
}

func TestHeaderRejectsMalformedAndTruncated(t *testing.T) {
	var valid bytes.Buffer
	if err := writeHeader(&valid, kindLink, Token{}, []byte{5}); err != nil {
		t.Fatal(err)
	}
	wire := valid.Bytes()
	for n := range len(wire) {
		if _, err := readHeader(bytes.NewReader(wire[:n])); err == nil {
			t.Fatalf("accepted %d-byte truncation", n)
		}
	}
	for _, index := range []int{0, 10, 11, 28} {
		malformed := append([]byte(nil), wire...)
		malformed[index] = 0
		if _, err := readHeader(bytes.NewReader(malformed)); err == nil {
			t.Fatalf("accepted malformed byte %d", index)
		}
	}
}

func TestHeaderReadsOnlyItsOwnBytes(t *testing.T) {
	var wire bytes.Buffer
	address := bytes.Repeat([]byte{7}, MaxAddressLen)
	if err := writeHeader(&wire, kindLink, Token{}, address); err != nil {
		t.Fatal(err)
	}
	wire.WriteString("session")
	h, err := readHeader(&wire)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(h.peer, address) {
		t.Fatal("address changed")
	}
	rest, err := io.ReadAll(&wire)
	if err != nil || string(rest) != "session" {
		t.Fatalf("%q %v", rest, err)
	}
}
