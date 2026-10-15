// Package bookmark implements the ordered v6 restart recovery record. Stored
// party bytes represent recovery rights, never active identities. Only Reclaim
// may transfer those rights into a live party after checking causal progress.
package bookmark

import (
	"bytes"
	"errors"

	"github.com/cirocosta/hearsay/before"
)

// DefaultSizeLimit bounds the complete encoded record, including its frame.
const DefaultSizeLimit = 16 << 20

// ErrReserved prevents reclaiming while a speculative bootstrap fork exists.
var ErrReserved = errors.New("bookmark: a fork is reserved")

type networkRecord struct {
	network    [16]byte
	written    before.Version
	identities [][]byte
}

// Record preserves networks newest first and identities oldest first.
// Its zero value is empty. Calls require external synchronization.
type Record struct{ networks []networkRecord }

// Clone copies mutable bookkeeping without activating recovery rights.
func (r *Record) Clone() *Record {
	cloned := &Record{networks: append([]networkRecord(nil), r.networks...)}
	for i := range cloned.networks {
		cloned.networks[i].identities = append([][]byte(nil), cloned.networks[i].identities...)
	}
	return cloned
}

func (r *Record) index(network [16]byte) int {
	for i := range r.networks {
		if r.networks[i].network == network {
			return i
		}
	}
	return -1
}

func (r *Record) promote(network [16]byte) *networkRecord {
	i := r.index(network)
	if i < 0 {
		r.networks = append(r.networks, networkRecord{})
		i = len(r.networks) - 1
		r.networks[i].network = network
	}
	entry := r.networks[i]
	copy(r.networks[1:i+1], r.networks[:i])
	r.networks[0] = entry
	return &r.networks[0]
}

// Record checkpoints only the live party's own progress and refreshes its
// identity's recency. It does not reclaim identities.
func (r *Record) Record(network [16]byte, live *before.Party, frontier before.Version) error {
	identity, err := live.MarshalBinary()
	if err != nil {
		return err
	}
	n := r.promote(network)
	n.written = n.written.Join(frontier.Project(live))
	kept := n.identities[:0]
	for _, old := range n.identities {
		if !bytes.Equal(old, identity) {
			kept = append(kept, old)
		}
	}
	n.identities = append(kept, identity)
	return nil
}

// Donate removes the donated region from every stored identity before the
// caller transmits it. The caller must durably store the resulting record first.
func (r *Record) Donate(network [16]byte, donation *before.Party) {
	if r.index(network) < 0 {
		return
	}
	n := r.promote(network)
	kept := make([][]byte, 0, len(n.identities))
	for _, raw := range n.identities {
		if remaining := party(raw).Without(donation); remaining != nil {
			encoded, _ := remaining.MarshalBinary()
			kept = append(kept, encoded)
		}
	}
	n.identities = kept
	if len(kept) == 0 {
		r.networks = r.networks[1:]
	}
}

// Reclaim joins only identities whose recorded own progress is dominated by
// frontier. noReservations must be true only while the caller's replica lock
// guarantees no speculative donation fork is outstanding or can be reserved.
func (r *Record) Reclaim(
	network [16]byte,
	live *before.Party,
	frontier before.Version,
	noReservations bool,
) error {
	if !noReservations {
		return ErrReserved
	}
	if r.index(network) < 0 {
		return nil
	}
	n := r.promote(network)
	waiting := make([][]byte, 0, len(n.identities))
	for _, raw := range n.identities {
		identity := party(raw)
		if !n.written.Project(identity).LessEqual(frontier) {
			waiting = append(waiting, raw)
			continue
		}
		if missing := identity.Without(live); missing != nil {
			if err := live.Join(missing); err != nil {
				panic(err)
			}
		}
	}
	n.identities = waiting
	return nil
}

func party(raw []byte) *before.Party {
	p, err := before.DecodeParty(raw)
	if err != nil {
		panic(err)
	}
	return p
}

func (n *networkRecord) compact() {
	var compacted before.Version
	for _, raw := range n.identities {
		compacted = compacted.Join(n.written.Project(party(raw)))
	}
	if len(compacted.AppendBinary(nil)) <= len(n.written.AppendBinary(nil)) {
		n.written = compacted
	}
}

// EncodeBounded compacts and evicts the oldest identity of the least recently
// used network until the entire frame fits. A limit below the empty-frame size
// is raised to that minimum. Eviction forfeits rights without reusing them.
func (r *Record) EncodeBounded(limit int) []byte {
	limit = max(limit, len(frame([]byte{0x80})))
	total := 0
	for i := range r.networks {
		r.networks[i].compact()
		total += len(encodeNetwork(nil, &r.networks[i]))
	}
	for recordSize(len(r.networks), total) > limit && len(r.networks) > 0 {
		n := &r.networks[len(r.networks)-1]
		total -= len(encodeNetwork(nil, n))
		if len(n.identities) > 0 {
			n.identities = n.identities[1:]
		}
		if len(n.identities) == 0 {
			r.networks = r.networks[:len(r.networks)-1]
		} else {
			n.compact()
			total += len(encodeNetwork(nil, n))
		}
	}
	return r.Encode()
}
