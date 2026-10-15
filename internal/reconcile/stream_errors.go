package reconcile

import (
	"errors"
	"io"

	"github.com/cirocosta/hearsay/internal/protocol"
	"github.com/cirocosta/hearsay/internal/sessionerr"
	"github.com/cirocosta/hearsay/observe"
)

func (s *streamSet) labelError(err error) error {
	index := -1
	var label *protocol.LabelError
	if errors.As(err, &label) && label.Stream < protocol.StreamCount {
		index = int(label.Stream)
	}
	return s.incomingError(index, err)
}

func (s *streamSet) incomingError(index int, err error) error {
	if index >= len(s.incoming) {
		index = -1
	}
	return sessionerr.Stream(observe.Role(s.remote), index, err)
}

func (s *streamSet) outgoingError(index int, err error) error {
	if index >= len(s.outgoing) {
		index = -1
	}
	return sessionerr.Stream(observe.Role(s.local), index, err)
}

type labelReader struct{ io.Reader }

func (r labelReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	return n, sessionerr.IO(sessionerr.Read, err)
}
