package tree

import (
	"math/rand/v2"
	"testing"

	"github.com/cirocosta/hearsay/before"
)

func TestApplyAgainstSequentialDistinctEffects(t *testing.T) {
	versions := versions(700)
	rng := rand.New(rand.NewPCG(39, 91))
	for trial := 0; trial < 50; trial++ {
		var original Tree[int]
		for _, i := range rng.Perm(400)[:200] {
			original = original.Insert(versions[i], i)
		}
		snapshot := original
		want := original
		var edits []Edit[int]
		for _, i := range rng.Perm(600)[:1+rng.IntN(250)] {
			insert := rng.IntN(2) == 0
			stamp := versions[600+rng.IntN(100)]
			edit := Edit[int]{Version: versions[i], Stamp: stamp, Value: i, Insert: insert}
			edits = append(edits, edit)
			if insert {
				want = want.Insert(edit.Version, edit.Value)
			} else {
				want = want.Forget(edit.Version, edit.Stamp)
			}
		}
		got := original.Apply(edits)
		validate(t, got.root, 0)
		if got.Hash() != want.Hash() || got.Len() != want.Len() ||
			!got.Latest().Equal(want.Latest()) {
			t.Fatalf("bulk effects disagree with sequential oracle on trial%d", trial)
		}
		if !snapshot.SameRoot(original) {
			t.Fatal("bulk mutation changed opening snapshot")
		}
		for version, value := range want.All() {
			found, ok := got.Get(version)
			if !ok || found != value {
				t.Fatal("bulk payload differs from oracle")
			}
		}
	}
}

func TestApplyKeepsUntouchedChildrenShared(t *testing.T) {
	versions := versions(2002)
	var original Tree[int]
	for i, version := range versions[:2000] {
		original = original.Insert(version, i)
	}
	a, b := Address(versions[2000]), Address(versions[2001])
	updated := original.Apply(
		[]Edit[int]{
			{Version: versions[2000], Value: 2000, Insert: true},
			{Version: versions[2001], Value: 2001, Insert: true},
		},
	)
	shared := 0
	for _, child := range original.root.children {
		if child.path[0] == a[0] || child.path[0] == b[0] {
			continue
		}
		if childAt(updated.root, child.path[0]) != child {
			t.Fatal("untouched radix lost structural sharing")
		}
		shared++
	}
	if shared < 100 {
		t.Fatal("sharing fixture lacks a broad root fan")
	}
}

func TestApplyNoOpsAndDuplicateRejection(t *testing.T) {
	versions := versions(5)
	original := Tree[int]{}.Insert(versions[0], 1)
	noOp := original.Apply(
		[]Edit[int]{
			{Version: versions[1], Stamp: versions[4]},
			{Version: versions[2], Stamp: versions[4]},
		},
	)
	if !original.SameRoot(noOp) {
		t.Fatal("missing keys changed tree or history")
	}
	old := original.Apply([]Edit[int]{{Version: versions[0], Stamp: before.Version{}}})
	if !old.SameRoot(original) {
		t.Fatal("causally older deletion applied")
	}
	defer func() {
		if recover() == nil {
			t.Fatal("duplicate edit addresses accepted")
		}
		if original.Len() != 1 {
			t.Fatal("invalid edits mutated snapshot")
		}
	}()
	original.Apply(
		[]Edit[int]{
			{Version: versions[0], Stamp: versions[3]},
			{Version: versions[0], Stamp: versions[4]},
		},
	)
}

func TestApplyCanonicalBulkBuildAndFullDelete(t *testing.T) {
	versions := versions(300)
	var empty Tree[int]
	var inserts []Edit[int]
	for i, version := range versions {
		inserts = append(inserts, Edit[int]{Version: version, Value: i, Insert: true})
	}
	filled := empty.Apply(inserts)
	validate(t, filled.root, 0)
	var deletes []Edit[int]
	for _, version := range versions {
		deletes = append(deletes, Edit[int]{Version: version, Stamp: versions[len(versions)-1]})
	}
	deleted := filled.Apply(deletes)
	if deleted.Len() != 0 || deleted.Hash() != empty.Hash() ||
		!deleted.Latest().Equal(filled.Latest()) {
		t.Fatal("full deletion lost history or retained nodes")
	}
}
