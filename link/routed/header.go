package routed

import (
	"bytes"
	"errors"
	"io"

	"github.com/cirocosta/hearsay/link"
)

const (
	// MaxAddressLen bounds the opaque advertised name in a LINK header.
	MaxAddressLen      = 255
	prefixLen          = 28
	kindLink      byte = 1
	kindStream    byte = 2
	ack           byte = 1
)

var magic = [10]byte{'R', 'O', 'U', 'T', 'E', 'D', 'L', 'I', 'N', 'K'}

// Token identifies one live link within an endpoint. It is routing state,
// never an authorization credential.
type Token [16]byte

type header struct {
	kind  byte
	token Token
	peer  []byte
}

func writeHeader(w io.Writer, kind byte, token Token, address []byte) error {
	var b [prefixLen + 1 + MaxAddressLen]byte
	copy(b[:10], magic[:])
	b[10] = 1
	b[11] = kind
	copy(b[12:28], token[:])
	n := prefixLen
	if kind == kindLink {
		b[n] = byte(len(address))
		n++
		n += copy(b[n:], address)
	}
	return writeRouting(w, b[:n])
}

func writeRouting(w io.Writer, b []byte) error {
	if err := writeAll(w, b); err != nil {
		return err
	}
	return link.Flush(w)
}

func writeAll(w io.Writer, b []byte) error {
	for len(b) > 0 {
		n, err := w.Write(b)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		b = b[n:]
	}
	return nil
}

func readHeader(r io.Reader) (header, error) {
	var b [prefixLen]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return header{}, err
	}
	if !bytes.Equal(b[:10], magic[:]) || b[10] != 1 || (b[11] != kindLink && b[11] != kindStream) {
		return header{}, errors.New("invalid routing header")
	}
	h := header{kind: b[11]}
	copy(h.token[:], b[12:])
	if h.kind == kindLink {
		var err error
		h.peer, err = readAddress(r)
		if err != nil {
			return header{}, err
		}
	}
	return h, nil
}

func readAddress(r io.Reader) ([]byte, error) {
	var n [1]byte
	if _, err := io.ReadFull(r, n[:]); err != nil {
		return nil, err
	}
	if n[0] == 0 {
		return nil, errors.New("empty advertised address")
	}
	address := make([]byte, int(n[0]))
	_, err := io.ReadFull(r, address)
	return address, err
}
