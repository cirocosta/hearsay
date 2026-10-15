package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"net"
	"net/netip"
	"os"
	"reflect"
	"sort"
	"sync/atomic"
	"time"

	h "github.com/cirocosta/hearsay"
	"github.com/cirocosta/hearsay/link"
	"github.com/cirocosta/hearsay/link/routed"
)

type scenario struct {
	provider string
	ctx      context.Context
	cancel   context.CancelFunc
	rust     *rust
	endpoint *routed.Endpoint
	address  string
	carrier  *link.Managed
	rumors   *h.Rumors[[]byte]
	victim   h.Version
	dials    *atomic.Uint64
	opened   uint64
}

func newScenario(provider string) (*scenario, error) {
	mode := "bootstrap"
	if provider == "rust" {
		mode = "seed"
	}
	oracle, err := startRust(mode)
	if err != nil {
		return nil, err
	}
	ep, address, dials, err := endpoint()
	if err != nil {
		oracle.close()
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	s := &scenario{
		provider: provider,
		ctx:      ctx,
		cancel:   cancel,
		rust:     oracle,
		endpoint: ep,
		address:  address,
		dials:    dials,
	}
	if err := s.connect(); err != nil {
		s.close()
		return nil, err
	}
	return s, nil
}

func (s *scenario) close() {
	s.cancel()
	if s.carrier != nil {
		_ = s.carrier.Close()
	}
	_ = s.endpoint.Close()
	s.rust.close()
}

func endpoint() (*routed.Endpoint, string, *atomic.Uint64, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, "", nil, err
	}
	address := listener.Addr().String()
	encoded, err := routed.EncodeTCPAddr(netip.MustParseAddrPort(address))
	if err != nil {
		_ = listener.Close()
		return nil, "", nil, err
	}
	dials := new(atomic.Uint64)
	dial := func(ctx context.Context, address []byte) (net.Conn, error) {
		conn, err := routed.TCPDial(ctx, address)
		if err == nil {
			dials.Add(1)
		}
		return conn, err
	}
	ep, err := routed.New(listener, encoded, dial, routed.Config{})
	if err != nil {
		_ = listener.Close()
	}
	return ep, address, dials, err
}

func (s *scenario) connect() error {
	var raw *routed.Link
	var err error
	rustFn := func() error {
		_, err := s.rust.command(map[string]any{"op": "connect", "address": s.address})
		return err
	}
	goFn := func() error { _, raw, err = s.endpoint.Accept(s.ctx); return err }
	if s.provider == "rust" {
		rustFn = func() error { _, err := s.rust.op("accept"); return err }
		goFn = func() error {
			encoded, e := routed.EncodeTCPAddr(netip.MustParseAddrPort(s.rust.address))
			if e != nil {
				return e
			}
			_, raw, err = s.endpoint.Link(s.ctx, encoded)
			return err
		}
	}
	if err := exchange(rustFn, goFn); err != nil {
		return err
	}
	s.carrier = link.Manage(raw)
	return nil
}

func bootstrap() *h.BootstrapConfig[[]byte] {
	b := h.Bootstrap(h.BytesCodec())
	if os.Getenv("HEARSAY_INTEROP_MINIMAL") != "1" {
		return b
	}
	settings := b.SynchronizationSettings()
	settings.SyncMemoryBudget = 1
	settings.TargetMessageSize = 0
	configured, err := b.WithSynchronizationSettings(settings)
	if err != nil {
		panic(err)
	}
	return configured
}

func seed() (*h.Rumors[[]byte], error) {
	p, err := h.Seed(h.BytesCodec())
	if err != nil {
		return nil, err
	}
	if os.Getenv("HEARSAY_INTEROP_MINIMAL") == "1" {
		settings := p.SynchronizationSettings()
		settings.SyncMemoryBudget = 1
		settings.TargetMessageSize = 0
		p, err = p.WithSynchronizationSettings(settings)
		if err != nil {
			return nil, err
		}
	}
	return p.IntoRumors()
}

func (s *scenario) join() error {
	joined, err := bootstrap().Join(s.ctx, s.carrier)
	if err != nil {
		return err
	}
	if joined.Peer == nil {
		return fmt.Errorf("bootstrap returned no peer")
	}
	s.rumors, err = joined.Peer.IntoRumors()
	return err
}

func (s *scenario) gossip() error {
	result, err := s.rumors.GossipOnce(s.ctx, s.carrier)
	s.opened += result.Stats.StreamsOpened
	return err
}
func (s *scenario) sync() error     { return s.rust.pair("gossip", s.gossip) }
func (s *scenario) check() error    { return check(s.rust, s.rumors) }
func hexVersion(v h.Version) string { b, _ := v.MarshalBinary(); return hex.EncodeToString(b) }
func check(r *rust, goPeer *h.Rumors[[]byte]) error {
	state, err := r.op("list")
	if err != nil {
		return err
	}
	snapshot := goPeer.Snapshot()
	records := make([]record, 0, snapshot.Len())
	for message := range snapshot.All() {
		if message.Err != nil {
			return message.Err
		}
		records = append(
			records,
			record{hexVersion(message.Version), hex.EncodeToString(message.Payload)},
		)
	}
	sort.Slice(records, func(i, j int) bool { return records[i].Version < records[j].Version })
	if !reflect.DeepEqual(records, state.Records) {
		return fmt.Errorf(
			"message set differs: Go %d records, Rust %d",
			len(records),
			len(state.Records),
		)
	}
	if latest := hexVersion(snapshot.Latest()); latest != state.Version {
		return fmt.Errorf("frontier differs: Go %s, Rust %s", latest, state.Version)
	}
	return nil
}
