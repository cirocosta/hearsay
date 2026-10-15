package tree

import (
	"bytes"
	"crypto/sha3"
	"errors"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/cirocosta/hearsay/before"
)

func leafViewHash(path Path, depth int) Hash {
	preimage := append([]byte{0, byte(32 - depth)}, path[depth:]...)
	full := sha3.Sum256(preimage)
	return Hash(full[:24])
}

func TestScopeSingletonVirtualHashes(t *testing.T) {
	v := versions(1)[0]
	tree := Tree[int]{}.Insert(v, 42)
	path := Address(v)
	for depth := 0; depth <= 32; depth++ {
		scope, err := tree.Scope(path[:depth])
		if err != nil || scope.Len() != 1 || scope.Depth() != depth {
			t.Fatal("logical scope missing")
		}
		if scope.Hash() != leafViewHash(path, depth) {
			t.Fatalf("wrong virtual leaf hash at depth %d", depth)
		}
		if !scope.Floor().Equal(v) || !scope.Ceiling().Equal(v) ||
			scope.MaxVersionBytes() != len(v.AppendBinary(nil)) {
			t.Fatal("scope bounds changed under rebase")
		}
		_, value, ok := scope.Leaf()
		if ok != (depth == 32) || ok && value != 42 {
			t.Fatal("logical leaf height mismatch")
		}
		children := scope.Children()
		if depth == 32 {
			if len(children) != 0 {
				t.Fatal("leaf has children")
			}
			continue
		}
		if len(children) != 1 || children[0].Radix != path[depth] ||
			children[0].Scope.Hash() != leafViewHash(path, depth+1) {
			t.Fatal("compressed child did not consume one radix")
		}
		rebuilt, err := scope.WithChildren(children)
		if err != nil || rebuilt.node != scope.node {
			t.Fatal("unchanged virtual rebuild lost node sharing")
		}
	}
}

func TestScopeCompressedBranchHashes(t *testing.T) {
	vs := versions(2)
	var a, b Path
	for i := 0; i < 8; i++ {
		a[i] = byte(i + 11)
		b[i] = a[i]
	}
	a[8], b[8] = 7, 200
	a[31], b[31] = 3, 9
	n := put(leaf(a, vs[0], 1, 0), leaf(b, vs[1], 2, 0), 0)
	tree := Tree[int]{root: n}
	for depth := 0; depth <= 8; depth++ {
		preimage := append([]byte{1, byte(8 - depth)}, a[depth:8]...)
		preimage = append(preimage, 0, 2)
		for _, path := range []Path{a, b} {
			hash := leafViewHash(path, 9)
			preimage = append(preimage, path[8])
			preimage = append(preimage, hash[:]...)
		}
		full := sha3.Sum256(preimage)
		scope, err := tree.Scope(a[:depth])
		if err != nil || scope.Hash() != Hash(full[:24]) {
			t.Fatalf("branch hash rebase failed at %d", depth)
		}
		children := scope.Children()
		if depth == 8 {
			if len(children) != 2 || children[0].Radix != 7 || children[1].Radix != 200 {
				t.Fatal("logical branch fan mismatch")
			}
			if children[0].Scope.Hash() != leafViewHash(a, 9) ||
				children[1].Scope.Hash() != leafViewHash(b, 9) {
				t.Fatal("logical fan child hash mismatch")
			}
		} else if len(children) != 1 || children[0].Radix != a[depth] {
			t.Fatal("compressed branch exposed fan too early")
		}
	}
	missing := append([]byte(nil), a[:9]...)
	missing[8] = 99
	scope, err := tree.Scope(missing)
	if err != nil || scope.Len() != 0 || scope.Hash() != emptyHash {
		t.Fatal("absent direct radix not empty")
	}
}

func TestBuildScopeSortedRuns(t *testing.T) {
	vs := versions(500)
	var want Tree[int]
	for i, v := range vs {
		want = want.Insert(v, i)
	}
	items := make([]Leaf[int], 0, len(vs))
	for v, value := range want.All() {
		items = append(items, Leaf[int]{Version: v, Value: value})
	}
	scope, err := BuildScope(nil, items)
	if err != nil || scope.Hash() != want.Hash() {
		t.Fatal("bulk builder changed canonical root")
	}
	got, err := scope.Tree(want.Latest())
	if err != nil {
		t.Fatal(err)
	}
	validate(t, got.root, 0)
	if got.Hash() != want.Hash() || !got.Latest().Equal(want.Latest()) {
		t.Fatal("root publication changed state")
	}
	path := Address(items[0].Version)
	for depth := 1; depth <= 32; depth++ {
		prefix := path[:depth]
		matching := slices.DeleteFunc(
			slices.Clone(items),
			func(item Leaf[int]) bool { p := Address(item.Version); return !bytes.HasPrefix(p[:], prefix) },
		)
		built, err := BuildScope(prefix, matching)
		if err != nil {
			t.Fatal(err)
		}
		existing, err := want.Scope(prefix)
		if err != nil || existing.Hash() != built.Hash() || existing.Len() != built.Len() {
			t.Fatal("bulk subtree differs from original scope")
		}
	}
	if _, err := BuildScope(nil, []Leaf[int]{items[0], items[0]}); !errors.Is(err, ErrScope) {
		t.Fatal("duplicate run accepted")
	}
	if _, err := BuildScope(nil, []Leaf[int]{items[1], items[0]}); !errors.Is(err, ErrScope) {
		t.Fatal("descending run accepted")
	}
	wrong := path
	wrong[0] ^= 255
	if _, err := BuildScope(wrong[:1], items[:1]); !errors.Is(err, ErrScope) {
		t.Fatal("out-of-scope run accepted")
	}
}

func TestScopeChildAssemblyValidation(t *testing.T) {
	vs := versions(30)
	var source Tree[int]
	for i, v := range vs {
		source = source.Insert(v, i)
	}
	root, err := source.Scope(nil)
	if err != nil {
		t.Fatal(err)
	}
	var empty Scope[int]
	rebuilt, err := empty.WithChildren(root.Children())
	if err != nil || rebuilt.Hash() != source.Hash() {
		t.Fatal("child fan assembly changed hash")
	}
	children := root.Children()
	children[0].Radix ^= 255
	if _, err := empty.WithChildren(children); !errors.Is(err, ErrScope) {
		t.Fatal("mismatched child radix accepted")
	}
	leaf := LeafScope(vs[0], 0)
	if _, err := leaf.WithChildren(nil); !errors.Is(err, ErrScope) {
		t.Fatal("bottom-height branch accepted")
	}
	if _, err := leaf.Tree(source.Latest()); !errors.Is(err, ErrScope) {
		t.Fatal("nonroot published as tree")
	}
	if _, err := source.Scope(make([]byte, 33)); !errors.Is(err, ErrScope) {
		t.Fatal("oversized prefix accepted")
	}
	child := root.Children()[0].Scope
	prefix := child.Prefix()
	prefix[0] ^= 255
	if child.Prefix()[0] == prefix[0] {
		t.Fatal("mutated prefix affected scope")
	}
}

func TestReplaceScopeAgainstFlatOracle(t *testing.T) {
	vs := versions(400)
	var original, remote Tree[int]
	for i, v := range vs[:250] {
		original = original.Insert(v, i)
	}
	for i, v := range vs[100:] {
		remote = remote.Insert(v, i+100)
	}
	rng := rand.New(rand.NewPCG(8, 97))
	for trial := 0; trial < 100; trial++ {
		path := Address(vs[rng.IntN(len(vs))])
		depth := rng.IntN(33)
		prefix := path[:depth]
		provider := remote
		if trial%3 == 0 {
			provider = Tree[int]{}
		}
		replacement, err := provider.Scope(prefix)
		if err != nil {
			t.Fatal(err)
		}
		got, err := original.ReplaceScope(replacement)
		if err != nil {
			t.Fatal(err)
		}
		var want Tree[int]
		for v, value := range original.All() {
			p := Address(v)
			if !bytes.HasPrefix(p[:], prefix) {
				want = want.Insert(v, value)
			}
		}
		for v, value := range provider.All() {
			p := Address(v)
			if bytes.HasPrefix(p[:], prefix) {
				want = want.Insert(v, value)
			}
		}
		if got.Hash() != want.Hash() || got.Len() != want.Len() {
			t.Fatalf("prefix splice disagrees with flat oracle at depth %d", depth)
		}
		if !original.Latest().LessEqual(got.Latest()) {
			t.Fatal("splice lost causal history")
		}
		validate(t, got.root, 0)
	}
}

func TestReplaceAbsentScopeReusesRoot(t *testing.T) {
	v := versions(1)[0]
	original := Tree[int]{}.Insert(v, 1)
	path := Address(v)
	path[0] ^= 255
	empty, err := (Tree[int]{}).Scope(path[:1])
	if err != nil {
		t.Fatal(err)
	}
	got, err := original.ReplaceScope(empty)
	if err != nil || !got.SameRoot(original) {
		t.Fatal("missing scope removal changed snapshot")
	}
	actual := Address(v)
	existing, err := original.Scope(actual[:])
	if err != nil {
		t.Fatal(err)
	}
	got, err = original.ReplaceScope(existing)
	if err != nil || !got.SameRoot(original) {
		t.Fatal("unchanged full-address splice lost sharing")
	}
}

func TestScopeAll(t *testing.T) {
	v := before.Version{}.Tick(before.SeedParty())
	scope := LeafScope(v, 123)
	count := 0
	for got, value := range scope.All() {
		if !got.Equal(v) || value != 123 {
			t.Fatal("scope walk changed leaf")
		}
		count++
	}
	if count != 1 {
		t.Fatal("scope walk missed leaf")
	}
}
