package observe

import "io"

// CaptureReader retains exactly the bytes delivered by its underlying reader.
// Construct one only for an observed exact-item read. Deliver Bytes only after
// the complete item has passed protocol validation; discard failed captures.
type CaptureReader struct {
	reader io.Reader
	bytes  []byte
}

// NewCaptureReader starts an empty capture around reader.
func NewCaptureReader(reader io.Reader) *CaptureReader { return &CaptureReader{reader: reader} }

// Read forwards a read and copies every delivered byte, including bytes returned
// alongside an error. It never reads ahead beyond the caller's requested bytes.
func (r *CaptureReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.bytes = append(r.bytes, p[:n]...)
	return n, err
}

// Bytes borrows the captured wire bytes until this reader is next read.
func (r *CaptureReader) Bytes() []byte { return r.bytes }
