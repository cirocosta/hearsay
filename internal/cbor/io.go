package cbor

import (
	"io"
	"unicode/utf8"
)

// ReadBytes reads a canonical byte string whose body is at most maximum bytes.
// It rejects oversized declarations before allocating the body.
func ReadBytes(r io.Reader, maximum int) ([]byte, error) {
	return readString(r, MajorBytes, maximum)
}

// ReadText reads a canonical text string and validates its UTF-8 encoding.
// maximum limits the encoded byte length, not the number of Unicode code points.
func ReadText(r io.Reader, maximum int) (string, error) {
	p, err := readString(r, MajorText, maximum)
	if err != nil {
		return "", err
	}
	if !utf8.Valid(p) {
		return "", ErrMalformed
	}
	return string(p), nil
}

func readString(r io.Reader, major uint8, maximum int) ([]byte, error) {
	h, err := ReadHead(r)
	if err != nil {
		return nil, err
	}
	if h.Major != major {
		return nil, ErrMajor
	}
	if maximum < 0 || h.Value > uint64(maximum) {
		return nil, ErrLimit
	}
	return ReadPayload(r, int(h.Value))
}

// ReadArray reads a canonical array head. Its members remain unread.
func ReadArray(r io.Reader) (uint64, error) {
	h, err := ReadHead(r)
	if err != nil {
		return 0, err
	}
	if h.Major != MajorArray {
		return 0, ErrMajor
	}
	return h.Value, nil
}

// WriteBytes writes a canonical byte string without copying its payload.
func WriteBytes(w io.Writer, p []byte) error {
	if err := WriteHead(w, MajorBytes, uint64(len(p))); err != nil {
		return err
	}
	n, err := w.Write(p)
	if err == nil && n != len(p) {
		return io.ErrShortWrite
	}
	return err
}

// WriteText writes a canonical text string. Invalid UTF-8 fails before writing.
func WriteText(w io.Writer, text string) error {
	if !utf8.ValidString(text) {
		return ErrMalformed
	}
	if err := WriteHead(w, MajorText, uint64(len(text))); err != nil {
		return err
	}
	n, err := io.WriteString(w, text)
	if err == nil && n != len(text) {
		return io.ErrShortWrite
	}
	return err
}
