package reconcile

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/cirocosta/hearsay/before"
	"github.com/cirocosta/hearsay/internal/protocol"
	"github.com/cirocosta/hearsay/internal/tree"
	"github.com/cirocosta/hearsay/link"
)

// replyBufferedLink models the Rust adapter's complete-reply publication
// barrier while retaining independently flow-controlled transport streams.
type replyBufferedLink struct {
	link.Link
	remote protocol.Role
}

func (l replyBufferedLink) Accept(ctx context.Context) (io.ReadCloser, error) {
	r, err := l.Link.Accept(ctx)
	if err != nil {
		return nil, err
	}
	// retain the label independently so the receiver routes this stream before
	// waiting for the first complete reply, just as the upstream adapter does.
	epoch, err := readLabelEpoch(r)
	if err != nil {
		_ = r.Close()
		return nil, err
	}
	return &bufferedReplyStream{
		reader: io.MultiReader(
			bytes.NewReader(epoch),
			&wholeReplyReader{source: r, role: l.remote},
		),
		source: r,
	}, nil
}

func readLabelEpoch(r io.Reader) ([]byte, error) {
	// tests use the first session's epoch. ReadLabel still checks exact label
	// spelling before reconstructing these canonical bytes.
	label, err := protocol.ReadLabel(r, 0)
	if err != nil {
		return nil, err
	}
	return label.AppendTo(nil)
}

type bufferedReplyStream struct {
	reader io.Reader
	source io.ReadCloser
}

func (r *bufferedReplyStream) Read(p []byte) (int, error) { return r.reader.Read(p) }
func (r *bufferedReplyStream) Close() error               { return r.source.Close() }

type wholeReplyReader struct {
	source io.Reader
	role   protocol.Role
	data   []byte
}

func (r *wholeReplyReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if len(r.data) == 0 {
		if err := r.bufferReply(); err != nil {
			return 0, err
		}
	}
	n := copy(p, r.data)
	r.data = r.data[n:]
	return n, nil
}

func (r *wholeReplyReader) bufferReply() error {
	for {
		frame, err := protocol.ReadFrame(
			r.source,
			r.role,
			protocol.FrameLimits{TargetMessageSize: protocol.MaxTargetMessageSize},
		)
		if err != nil {
			return err
		}
		r.data, err = frame.AppendTo(r.data, r.role)
		if err != nil {
			return err
		}
		if frame.Signal%2 == 1 || frame.Signal == protocol.ReplyEnd {
			return nil
		}
	}
}

func TestMinimalQueuesWithWholeReplyPeer(t *testing.T) {
	aClock := before.Seed()
	bClock := aClock.Fork()
	var a, b tree.Tree[[]byte]
	for i := 0; i < 41; i++ {
		a = a.Insert(aClock.Tick(), []byte{0xf6})
	}
	for i := 0; i < 40; i++ {
		b = b.Insert(bClock.Tick(), []byte{0xf6})
	}
	// the smaller side initiates. The responder's root reply contains more
	// questions than fit in its one-slot local queue. It must finish writing
	// that reply even while the semantic producer waits to publish questions.
	left, right := link.MemoryWithCapacity(1)
	defer func() { _ = left.Close() }()
	ra, rb := runOnPair(
		t,
		replyBufferedLink{Link: left, remote: protocol.Initiator},
		replyBufferedLink{Link: right, remote: protocol.Responder},
		0,
		a,
		b,
		0,
	)
	want := a.Join(b)
	sameTree(t, ra.Tree, want)
	sameTree(t, rb.Tree, want)
}
