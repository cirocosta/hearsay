// Package testlink provides a calibrated, paced transport for protocol tests
// and benchmarks. It is not a production network transport.
package testlink

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cirocosta/hearsay/link"
)

// Config controls one connected pair. RTT applies equally in both directions;
// BytesPerSecond is the shared rate in each direction, with zero unpaced.
// StreamWindow bounds copied, undelivered wire bytes independently per stream.
// Zero windows and packets default to 16 MiB and 16 KiB respectively. The base
// MemoryLink adds a further 8 KiB of delivered, unread buffering per stream.
// Control has its own window and shares its direction's serialization clock.
type Config struct {
	RTT            time.Duration
	BytesPerSecond uint64
	StreamWindow   int
	PacketSize     int
}

// Stats reports transport-owned wire bytes, including labels and control.
// QueuedBytes includes the packet currently waiting for delivery or writing to
// the base link. DeliveredBytes counts bytes accepted by the base MemoryLink,
// rather than bytes consumed by the protocol or application.
type Stats struct {
	QueuedBytes     uint64
	PeakQueuedBytes uint64
	DeliveredBytes  uint64
}

type counters struct {
	queued    atomic.Uint64
	peak      atomic.Uint64
	delivered atomic.Uint64
}

func (c *counters) add(n int) {
	current := c.queued.Add(uint64(n))
	peak := c.peak.Load()
	for current > peak {
		if c.peak.CompareAndSwap(peak, current) {
			return
		}
		peak = c.peak.Load()
	}
}

func (c *counters) release(n int) { c.queued.Add(0 - uint64(n)) }
func (c *counters) snapshot() Stats {
	return Stats{
		QueuedBytes:     c.queued.Load(),
		PeakQueuedBytes: c.peak.Load(),
		DeliveredBytes:  c.delivered.Load(),
	}
}

type pair struct {
	ctx        context.Context
	cancel     context.CancelFunc
	config     Config
	base       [2]*link.MemoryLink
	directions [2]direction
	stats      [2]counters
	mu         sync.Mutex
	closed     bool
	closeOnce  sync.Once
	closeErr   error
	workers    sync.WaitGroup
}

// Link is one endpoint of a scheduled MemoryLink pair. Closing either endpoint
// aborts both directions, unblocks all I/O, and joins every delivery worker.
// Data-stream Close and Complete enqueue a boundary and return immediately;
// workers drain accepted bytes before delegating that boundary to the base
// stream. Concurrent sessions must still be serialized.
type Link struct {
	pair    *pair
	side    int
	control *control
}

var _ link.Link = (*Link)(nil)

// Pair creates two connected endpoints. PacketSize must fit StreamWindow.
// It starts two control-delivery workers; each opened data stream starts one
// additional worker, which exits when that stream drains and closes.
func Pair(config Config) (*Link, *Link, error) {
	config, err := normalize(config)
	if err != nil {
		return nil, nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	left, right := link.Memory()
	p := &pair{ctx: ctx, cancel: cancel, config: config, base: [2]*link.MemoryLink{left, right}}
	for i := range p.directions {
		p.directions[i] = direction{rate: config.BytesPerSecond, propagation: config.RTT / 2}
	}
	a, b := &Link{pair: p, side: 0}, &Link{pair: p, side: 1}
	for _, endpoint := range []*Link{a, b} {
		base := p.base[endpoint.side].Control()
		writer, err := p.writer(endpoint.side, base)
		if err != nil {
			_ = endpoint.Close()
			return nil, nil, err
		}
		endpoint.control = &control{owner: endpoint, reader: base, writer: writer}
	}
	return a, b, nil
}

func normalize(config Config) (Config, error) {
	if config.RTT < 0 || config.StreamWindow < 0 || config.PacketSize < 0 {
		return config, errors.New("testlink: negative configuration")
	}
	if config.StreamWindow == 0 {
		config.StreamWindow = 16 << 20
	}
	if config.PacketSize == 0 {
		config.PacketSize = min(16<<10, config.StreamWindow)
	}
	if config.PacketSize > config.StreamWindow {
		return config, errors.New("testlink: packet exceeds stream window")
	}
	return config, nil
}

// Control returns the persistent duplex control stream.
func (l *Link) Control() io.ReadWriteCloser { return l.control }

// Open opens an independently flow-controlled, scheduled outgoing stream.
func (l *Link) Open(ctx context.Context) (io.WriteCloser, error) {
	base, err := l.pair.base[l.side].Open(ctx)
	if err != nil {
		return nil, err
	}
	writer, err := l.pair.writer(l.side, base)
	if err != nil {
		_ = base.Close()
		return nil, err
	}
	return writer, nil
}

// Accept receives the next incoming stream without changing its read semantics.
func (l *Link) Accept(ctx context.Context) (io.ReadCloser, error) {
	return l.pair.base[l.side].Accept(ctx)
}

// Stats returns this endpoint's outgoing transport counters.
func (l *Link) Stats() Stats { return l.pair.stats[l.side].snapshot() }

// Close aborts the pair and waits for all delivery workers. It is idempotent.
func (l *Link) Close() error {
	l.pair.abort()
	l.pair.workers.Wait()
	return l.pair.closeErr
}

func (p *pair) abort() {
	p.closeOnce.Do(func() {
		p.mu.Lock()
		p.closed = true
		p.cancel()
		p.mu.Unlock()
		p.closeErr = p.base[0].Close()
	})
}

func (p *pair) writer(side int, base io.WriteCloser) (*writer, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, io.ErrClosedPipe
	}
	w := &writer{
		owner:   p,
		side:    side,
		base:    base,
		changed: make(chan struct{}),
		done:    make(chan struct{}),
	}
	p.workers.Add(1)
	go w.deliver()
	return w, nil
}

type control struct {
	owner  *Link
	reader io.Reader
	writer *writer
}

func (c *control) Read(p []byte) (int, error)  { return c.reader.Read(p) }
func (c *control) Write(p []byte) (int, error) { return c.writer.Write(p) }
func (c *control) Flush() error                { return c.writer.Flush() }
func (c *control) Close() error                { return c.owner.Close() }
