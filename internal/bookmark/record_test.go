package bookmark

import (
	"bytes"
	"errors"
	"testing"

	"github.com/cirocosta/hearsay/before"
)

func TestOwnProgressAndRefresh(t *testing.T) {
	clock := before.Seed()
	remote := clock.Fork()
	remote.Ticks(10)
	clock.Absorb(remote.Version())
	clock.Tick()
	r := new(Record)
	network := [16]byte{1}
	if err := r.Record(network, clock.Party(), clock.Version()); err != nil {
		t.Fatal(err)
	}
	n := &r.networks[0]
	if !n.written.Equal(clock.OwnVersion()) {
		t.Fatal("recorded remote progress")
	}
	if err := r.Record(network, remote.Party(), remote.Version()); err != nil {
		t.Fatal(err)
	}
	if err := r.Record(network, clock.Party(), clock.Version()); err != nil {
		t.Fatal(err)
	}
	if len(n.identities) != 2 {
		t.Fatal("duplicated identity")
	}
	encoded, err := clock.Party().MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(n.identities[1], encoded) {
		t.Fatal("identity recency lost")
	}
}

func TestReclaimAndDonation(t *testing.T) {
	old := before.Seed()
	live := old.Fork()
	old.Ticks(7)
	network := [16]byte{1}
	r := new(Record)
	if err := r.Record(network, old.Party(), old.Version()); err != nil {
		t.Fatal(err)
	}
	old.IntoParts() // the old incarnation can no longer tick.
	if err := r.Reclaim(network, live.Party(), before.Version{}, true); err != nil {
		t.Fatal(err)
	}
	if live.Party().IsSeed() {
		t.Fatal("reclaimed before catching up")
	}
	frontier := r.networks[0].written
	if err := r.Reclaim(network, live.Party(), frontier, false); !errors.Is(err, ErrReserved) {
		t.Fatal(err)
	}
	if live.Party().IsSeed() {
		t.Fatal("reclaimed with a reservation")
	}
	if err := r.Reclaim(network, live.Party(), frontier, true); err != nil {
		t.Fatal(err)
	}
	if !live.Party().IsSeed() {
		t.Fatal("failed to reclaim caught-up identity")
	}
	if err := r.Record(network, live.Party(), frontier); err != nil {
		t.Fatal(err)
	}
	donation := live.Party().Fork()
	r.Donate(network, donation)
	for _, raw := range r.networks[0].identities {
		if !party(raw).IsDisjoint(donation) {
			t.Fatal("donation still recoverable")
		}
	}
	r.Donate(network, live.Party())
	if len(r.networks) != 0 {
		t.Fatal("empty network retained after donation")
	}
}

func TestRetentionOrderingAndClone(t *testing.T) {
	r := sample(t)
	oldest := r.networks[0].network
	other := [16]byte{1}
	clock := before.Seed()
	if err := r.Record(other, clock.Party(), clock.Version()); err != nil {
		t.Fatal(err)
	}
	cloned := r.Clone()
	full := r.EncodeBounded(DefaultSizeLimit)
	if len(
		full,
	) != recordSize(
		len(r.networks),
		len(encodeNetwork(nil, &r.networks[0]))+len(encodeNetwork(nil, &r.networks[1])),
	) {
		t.Fatal("frame size estimate differs")
	}
	limit := len(full) - 1
	encoded := r.EncodeBounded(limit)
	if len(encoded) > limit {
		t.Fatal("retention exceeded limit")
	}
	if r.networks[0].network != other {
		t.Fatal("evicted newest network first")
	}
	if r.index(oldest) >= 0 && len(r.networks[r.index(oldest)].identities) >= 3 {
		t.Fatal("oldest identity retained")
	}
	if len(cloned.networks[1].identities) != 3 {
		t.Fatal("clone shared mutable order")
	}
	reloaded, err := Decode(cloned.Encode())
	if err != nil {
		t.Fatal(err)
	}
	if reloaded.networks[0].network != other || reloaded.networks[1].network != oldest {
		t.Fatal("network LRU order lost")
	}
	if got := r.EncodeBounded(0); !bytes.Equal(got, unhex(t, emptyFixture)) {
		t.Fatalf("minimum frame: %x", got)
	}
}

func TestOverlappingRightsShareWriteFrontier(t *testing.T) {
	old := before.Seed()
	network := [16]byte{1}
	r := new(Record)
	if err := r.Record(network, old.Party(), old.Version()); err != nil {
		t.Fatal(err)
	}
	// A speculative fork leaves an older full identity beside the newer left
	// identity. Writes under the newer identity must constrain the older one.
	live := old.Fork()
	old.Ticks(2)
	if err := r.Record(network, old.Party(), old.Version()); err != nil {
		t.Fatal(err)
	}
	_, written := old.IntoParts()
	if err := r.Reclaim(network, live.Party(), live.Version(), true); err != nil {
		t.Fatal(err)
	}
	if live.Party().IsSeed() {
		t.Fatal("older overlapping identity bypassed newer writes")
	}
	if len(r.networks[0].identities) != 2 {
		t.Fatal("uncaught rights were removed")
	}
	if err := r.Reclaim(network, live.Party(), written, true); err != nil {
		t.Fatal(err)
	}
	if !live.Party().IsSeed() || len(r.networks[0].identities) != 0 {
		t.Fatal("caught-up overlapping rights were not reclaimed")
	}
}
