package main

import (
	"cmp"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/netip"
	"slices"
	"time"

	"github.com/cirocosta/hearsay"
)

// envelope carries multiple application payload kinds in one Hearsay network.
type envelope struct {
	Kind   string `json:"kind"`
	Member record `json:"member,omitzero"`
	Card   card   `json:"card,omitzero"`
}

func (e envelope) validate() error {
	switch e.Kind {
	case "member":
		if _, err := netip.ParseAddrPort(e.Member.Address); err != nil {
			return fmt.Errorf("member address: %w", err)
		}
		if e.Member.Incarnation == "" || e.Member.Generation == 0 || e.Member.Counter == 0 {
			return errors.New("member requires incarnation, generation and counter")
		}
	case "todo", "in_progress", "done":
		return e.Card.validate(e.Kind)
	default:
		return fmt.Errorf("unknown payload kind %q", e.Kind)
	}
	return nil
}

// payloadCodec validates local writes and received records at the same boundary.
func payloadCodec() hearsay.Codec[envelope] {
	codec := hearsay.JSONCodec[envelope]()
	encode, decode := codec.Encode, codec.DecodeBounded
	codec.Encode = func(e envelope) ([]byte, error) {
		if err := e.validate(); err != nil {
			return nil, err
		}
		return encode(e)
	}
	codec.DecodeBounded = func(data []byte, limit uint64) (envelope, error) {
		e, err := decode(data, limit)
		if err != nil {
			return envelope{}, err
		}
		return e, e.validate()
	}
	return codec
}

// record is written only by the process advertising address. Suspicion is local.
type record struct {
	Address     string `json:"address"`
	Name        string `json:"name"`
	Generation  uint64 `json:"generation"`
	Incarnation string `json:"incarnation"`
	Counter     uint64 `json:"counter"`
	Left        bool   `json:"left,omitzero"`
}

type member struct {
	Record record
	Seen   time.Time
}

func compareIncarnation(a, b record) int {
	if c := cmp.Compare(a.Generation, b.Generation); c != 0 {
		return c
	}
	return cmp.Compare(a.Incarnation, b.Incarnation)
}

func (m member) status(now time.Time, suspect, unreachable time.Duration) string {
	if m.Record.Left {
		return "left"
	}
	if now.Sub(m.Seen) >= unreachable {
		return "unreachable"
	}
	if now.Sub(m.Seen) >= suspect {
		return "suspected"
	}
	return "active"
}

// observe ignores replayed counters, old incarnations and heartbeats after leave.
func observe(members map[string]member, r record, now time.Time) {
	if r.Address == "" || r.Incarnation == "" || r.Counter == 0 {
		return
	}
	old, exists := members[r.Address]
	order := compareIncarnation(r, old.Record)
	if exists && (order < 0 || order == 0 && (old.Record.Left || r.Counter <= old.Record.Counter)) {
		return
	}
	members[r.Address] = member{Record: r, Seen: now}
}

func newRecord(replica *hearsay.Replica[envelope], address string) record {
	var generation uint64
	for m := range replica.Snapshot().All() {
		if m.Payload.Kind == "member" && m.Payload.Member.Address == address {
			generation = max(generation, m.Payload.Member.Generation)
		}
	}
	id := make([]byte, 16)
	_, _ = rand.Read(id)
	return record{Address: address, Generation: generation + 1, Incarnation: hex.EncodeToString(id)}
}

// publish atomically replaces this address's records instead of growing a log.
func publish(replica *hearsay.Replica[envelope], self *record) error {
	next, stale, err := memberUpdate(replica.Snapshot(), *self)
	if err != nil {
		return err
	}
	err = replica.Batch(func(b *hearsay.Batch[envelope]) error {
		if err := b.RedactAll(stale); err != nil {
			return err
		}
		return b.Send(envelope{Kind: "member", Member: next})
	})
	if err == nil {
		*self = next
	}
	return err
}

func memberUpdate(
	snapshot hearsay.Snapshot[envelope],
	self record,
) (record, []hearsay.Version, error) {
	next := self
	var stale []hearsay.Version
	for m := range snapshot.All() {
		if m.Err != nil {
			return record{}, nil, fmt.Errorf("read member record: %w", m.Err)
		}
		if m.Payload.Kind != "member" || m.Payload.Member.Address != self.Address {
			continue
		}
		// rebasing and removal use this same snapshot. A competing incarnation
		// learned afterward stays live until the next heartbeat can inspect it.
		if compareIncarnation(m.Payload.Member, next) > 0 {
			next.Generation = m.Payload.Member.Generation + 1
		}
		stale = append(stale, m.Version)
	}
	return next, stale, nil
}

func sortedAddresses(members map[string]member) []string {
	addresses := make([]string, 0, len(members))
	for address := range members {
		addresses = append(addresses, address)
	}
	slices.Sort(addresses)
	return addresses
}
