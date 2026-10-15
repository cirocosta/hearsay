package tree

import (
	"bytes"
	"crypto/sha3"
	"fmt"
	"math/big"
	"math/rand/v2"
	"sync"
	"testing"

	"github.com/cirocosta/hearsay/before"
)

func versions(n int) []before.Version {
	p := before.SeedParty()
	v := before.Version{}
	out := make([]before.Version, n)
	for i := range out {
		v = v.Tick(p)
		out[i] = v
	}
	return out
}

func validate[T any](t *testing.T, n *node[T], base uint8) {
	t.Helper()
	if n == nil {
		return
	}
	if n.base != base || n.depth < base || n.depth > 32 {
		t.Fatalf("invalid depth: base %d depth %d", n.base, n.depth)
	}
	if n.entry != nil {
		if len(n.children) != 0 || n.depth != 32 || n.count != 1 {
			t.Fatal("invalid leaf")
		}
		if !n.floor.Equal(n.entry.version) || !n.ceiling.Equal(n.entry.version) {
			t.Fatal("invalid leaf bounds")
		}
		if n.versionBytes != len(n.entry.version.AppendBinary(nil)) {
			t.Fatal("invalid leaf encoding size")
		}
	} else {
		if len(n.children) < 2 || len(n.children) > 256 || n.depth == 32 {
			t.Fatal("invalid branch fan")
		}
		count := 0
		floor := n.children[0].floor
		ceiling := before.Version{}
		maxBytes := 0
		for i, c := range n.children {
			if !bytes.Equal(c.path[base:n.depth], n.path[base:n.depth]) {
				t.Fatal("prefix mismatch")
			}
			if i > 0 && n.children[i-1].path[n.depth] >= c.path[n.depth] {
				t.Fatal("unordered fan")
			}
			validate(t, c, n.depth+1)
			count += c.count
			floor = floor.Meet(c.floor)
			ceiling = ceiling.Join(c.ceiling)
			maxBytes = max(maxBytes, c.versionBytes)
		}
		if count != n.count || !floor.Equal(n.floor) || !ceiling.Equal(n.ceiling) {
			t.Fatal("invalid aggregates")
		}
		maxBytes = max(maxBytes, len(floor.AppendBinary(nil)), len(ceiling.AppendBinary(nil)))
		if n.versionBytes != maxBytes {
			t.Fatal("invalid encoding size aggregate")
		}
	}
	rebuilt := *n
	rehash(&rebuilt)
	if rebuilt.hash != n.hash {
		t.Fatal("invalid hash memo")
	}
}

func TestVersionBoundSizeShrinksAfterRedaction(t *testing.T) {
	p := before.SeedParty()
	small := before.Version{}.Tick(p)
	large := small.TicksBig(p, new(big.Int).Lsh(big.NewInt(1), 512))
	tree := Tree[int]{}.Insert(small, 1).Insert(large, 2)
	if tree.MaxVersionBytes() <= len(small.AppendBinary(nil)) {
		t.Fatal("large version did not expand size bound")
	}
	tree = tree.Forget(large, large)
	if tree.MaxVersionBytes() != len(small.AppendBinary(nil)) {
		t.Fatal("redaction retained obsolete encoding size")
	}
	if !tree.Latest().Equal(large) {
		t.Fatal("encoding size update changed history")
	}
	if tree.Forget(small, large).MaxVersionBytes() != 0 {
		t.Fatal("empty nodes retained encoding size")
	}
}

func TestCanonicalInsertionAndDeletion(t *testing.T) {
	vs := versions(400)
	var canonical Tree[int]
	for i, v := range vs {
		canonical = canonical.Insert(v, i)
	}
	rng := rand.New(rand.NewPCG(1, 2))
	for trial := 0; trial < 8; trial++ {
		var tree Tree[int]
		for _, i := range rng.Perm(len(vs)) {
			tree = tree.Insert(vs[i], i)
		}
		validate(t, tree.root, 0)
		if tree.Hash() != canonical.Hash() {
			t.Fatal("insertion order changed hash")
		}
		snapshot := tree
		for _, i := range rng.Perm(len(vs))[:200] {
			tree = tree.Forget(vs[i], tree.Latest())
		}
		validate(t, tree.root, 0)
		var rebuilt Tree[int]
		for v, value := range tree.All() {
			rebuilt = rebuilt.Insert(v, value)
		}
		if rebuilt.Hash() != tree.Hash() {
			t.Fatal("deletion did not maximally compress")
		}
		if snapshot.Hash() != canonical.Hash() || snapshot.Len() != 400 {
			t.Fatal("mutation changed snapshot")
		}
	}
}

func TestCompressionAtEveryDepth(t *testing.T) {
	v := versions(2)
	for d := uint8(0); d < 32; d++ {
		var a, b Path
		b[d] = 1
		x := put(leaf(a, v[0], 0, 0), leaf(b, v[1], 1, 0), 0)
		validate(t, x, 0)
		if x.depth != d {
			t.Fatalf("depth got %d want %d", x.depth, d)
		}
		y := remove(x, b)
		validate(t, y, 0)
		if y.entry == nil || y.base != 0 || y.hash != leaf(a, v[0], 0, 0).hash {
			t.Fatal("singleton failed compression")
		}
		if remove(y, a) != nil {
			t.Fatal("final deletion retained nodes")
		}
	}
	var root *node[int]
	for i := 0; i < 256; i++ {
		var p Path
		p[0] = byte(i)
		root = put(root, leaf(p, v[0], i, 0), 0)
	}
	validate(t, root, 0)
	if len(root.children) != 256 {
		t.Fatal("saturated fan lost leaves")
	}
}

func TestMerklePreimages(t *testing.T) {
	var empty Tree[int]
	full := sha3.Sum256([]byte{1, 0, 0, 0})
	if empty.Hash() != Hash(full[:24]) {
		t.Fatal("empty hash")
	}
	v := versions(1)[0]
	p := Address(v)
	tree := empty.Insert(v, 1)
	input := append([]byte{0, 32}, p[:]...)
	full = sha3.Sum256(input)
	if tree.Hash() != Hash(full[:24]) {
		t.Fatal("leaf preimage")
	}
	if tree.Insert(v, 999).root != tree.root {
		t.Fatal("duplicate replaced immutable leaf")
	}
	if tree.Insert(v, 999).Hash() != empty.Insert(v, 42).Hash() {
		t.Fatal("payload entered hash")
	}
	var a, b Path
	a[0] = 7
	b[0] = 200
	n := put(leaf(a, v, 1, 0), leaf(b, v, 2, 0), 0)
	input = []byte{1, 0, 0, 2}
	for _, c := range n.children {
		input = append(input, c.path[0])
		input = append(input, c.hash[:]...)
	}
	full = sha3.Sum256(input)
	if n.hash != Hash(full[:24]) {
		t.Fatal("branch preimage")
	}
}

func TestHistoryAndNoOp(t *testing.T) {
	p := before.SeedParty()
	v := before.Version{}.Tick(p)
	deletion := v.Tick(p)
	tree := Tree[int]{}.Insert(v, 1)
	forgotten := tree.Forget(v, deletion)
	if forgotten.Len() != 0 || !forgotten.Latest().Equal(deletion) {
		t.Fatal("lost redaction history")
	}
	if forgotten.Join(tree).Len() != 0 || tree.Join(forgotten).Len() != 0 {
		t.Fatal("resurrected redaction")
	}
	if !forgotten.SameRoot(forgotten.Forget(v, deletion.Tick(p))) {
		t.Fatal("missing deletion advanced history")
	}
	if !tree.SameRoot(tree.Forget(v, before.Version{})) {
		t.Fatal("older deletion changed tree")
	}
	if !tree.SameRoot(tree.Join(tree)) {
		t.Fatal("identical join did not share root")
	}
}

type modelEntry struct {
	v     before.Version
	value int
}
type model struct {
	live    map[string]modelEntry
	ceiling before.Version
}

func key(v before.Version) string { return string(v.AppendBinary(nil)) }
func newModel() model             { return model{live: map[string]modelEntry{}} }
func modelJoin(a, b model) model {
	out := newModel()
	out.ceiling = a.ceiling.Join(b.ceiling)
	for k, e := range a.live {
		if _, ok := b.live[k]; ok || !e.v.LessEqual(b.ceiling) {
			out.live[k] = e
		}
	}
	for k, e := range b.live {
		if _, ok := a.live[k]; ok || !e.v.LessEqual(a.ceiling) {
			out.live[k] = e
		}
	}
	return out
}

func checkModel(t *testing.T, tree Tree[int], m model) {
	t.Helper()
	validate(t, tree.root, 0)
	if tree.Len() != len(m.live) || !tree.Latest().Equal(m.ceiling) {
		t.Fatalf("replica mismatch: got %d want %d", tree.Len(), len(m.live))
	}
	for v, value := range tree.All() {
		e, ok := m.live[key(v)]
		if !ok || value != e.value {
			t.Fatal("unexpected entry")
		}
		if got, ok := tree.Get(v); !ok || got != value {
			t.Fatal("lookup mismatch")
		}
	}
	var canonical Tree[int]
	for _, e := range m.live {
		canonical = canonical.Insert(e.v, e.value)
	}
	if canonical.Hash() != tree.Hash() {
		t.Fatal("merge produced noncanonical tree")
	}
}

func TestRandomReplicaHistoriesAgainstFlatOracle(t *testing.T) {
	for seed := uint64(0); seed < 8; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(seed, 97))
			parties := []*before.Party{before.SeedParty()}
			for len(parties) < 5 {
				parties = append(parties, parties[0].Fork())
			}
			trees := make([]Tree[int], 5)
			models := make([]model, 5)
			for i := range models {
				models[i] = newModel()
			}
			for step := 0; step < 300; step++ {
				i := rng.IntN(5)
				old := trees[i]
				oldModel := models[i]
				switch rng.IntN(4) {
				case 0, 1:
					v := trees[i].Latest().Tick(parties[i])
					trees[i] = trees[i].Insert(v, step)
					m := newModel()
					for k, e := range models[i].live {
						m.live[k] = e
					}
					m.live[key(v)] = modelEntry{v, step}
					m.ceiling = v
					models[i] = m
				case 2:
					if len(models[i].live) > 0 {
						for k, e := range models[i].live {
							d := trees[i].Latest().Tick(parties[i])
							trees[i] = trees[i].Forget(e.v, d)
							m := newModel()
							for k, e := range models[i].live {
								m.live[k] = e
							}
							delete(m.live, k)
							m.ceiling = d
							models[i] = m
							break
						}
					}
				case 3:
					j := rng.IntN(5)
					forward := trees[i].Join(trees[j])
					reverse := trees[j].Join(trees[i])
					if forward.Hash() != reverse.Hash() ||
						!forward.Latest().Equal(reverse.Latest()) {
						t.Fatal("join not commutative")
					}
					trees[i] = forward
					models[i] = modelJoin(models[i], models[j])
				}
				checkModel(t, trees[i], models[i])
				checkModel(t, old, oldModel)
			}
		})
	}
}

func TestRangePruningAndEarlyStop(t *testing.T) {
	vs := versions(100)
	var tree Tree[int]
	for i, v := range vs {
		tree = tree.Insert(v, i)
	}
	count := 0
	cutoff := vs[49]
	for _, value := range tree.Range(func(v before.Version) bool { return v.LessEqual(cutoff) }, func(floor, ceiling before.Version) int {
		if ceiling.LessEqual(cutoff) {
			return 1
		}
		if !floor.LessEqual(cutoff) {
			return -1
		}
		return 0
	}) {
		if value >= 50 {
			t.Fatal("range admitted outside bound")
		}
		count++
	}
	if count != 50 {
		t.Fatalf("range got %d", count)
	}
	count = 0
	for range tree.All() {
		count++
		break
	}
	if count != 1 {
		t.Fatal("iterator ignored stop")
	}
}

func TestConcurrentSnapshots(t *testing.T) {
	vs := versions(300)
	var original Tree[int]
	for i, v := range vs[:100] {
		original = original.Insert(v, i)
	}
	want := original.Hash()
	var wg sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		wg.Go(func() {
			snapshot := original
			for i, v := range vs[100:] {
				snapshot = snapshot.Insert(v, i)
				if i%3 == 0 {
					snapshot = snapshot.Forget(v, vs[len(vs)-1])
				}
				for range original.All() {
				}
				if original.Hash() != want || original.Len() != 100 {
					t.Error("shared snapshot changed")
					return
				}
			}
		})
	}
	wg.Wait()
}
