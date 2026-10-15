package tree

import (
	"fmt"
	"testing"

	"github.com/cirocosta/hearsay/before"
)

var (
	benchmarkTree Tree[int]
	benchmarkHash Hash
)

func benchmarkFixture(n int) (Tree[int], []before.Version) {
	vs := versions(n)
	var t Tree[int]
	for i, v := range vs {
		t = t.Insert(v, i)
	}
	return t, vs
}

func BenchmarkSnapshot(b *testing.B) {
	t, _ := benchmarkFixture(10000)
	b.ReportAllocs()
	for b.Loop() {
		benchmarkTree = t
	}
}

func BenchmarkHash(b *testing.B) {
	t, _ := benchmarkFixture(10000)
	b.ReportAllocs()
	for b.Loop() {
		benchmarkHash = t.Hash()
	}
}

func BenchmarkLookup(b *testing.B) {
	for _, size := range []int{100, 10000} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			t, vs := benchmarkFixture(size)
			i := 0
			b.ReportAllocs()
			for b.Loop() {
				_, _ = t.Get(vs[i%size])
				i++
			}
		})
	}
}

func BenchmarkInsert(b *testing.B) {
	t, _ := benchmarkFixture(10000)
	p := before.SeedParty()
	v := t.Latest().Tick(p)
	b.ReportAllocs()
	for b.Loop() {
		benchmarkTree = t.Insert(v, 0)
	}
}

func BenchmarkForget(b *testing.B) {
	t, vs := benchmarkFixture(10000)
	v := t.Latest().Tick(before.SeedParty())
	b.ReportAllocs()
	for b.Loop() {
		benchmarkTree = t.Forget(vs[5000], v)
	}
}

func BenchmarkJoin(b *testing.B) {
	t, vs := benchmarkFixture(10000)
	p := before.SeedParty()
	v := t.Latest().Tick(p)
	for name, other := range map[string]Tree[int]{
		"equal":             t,
		"one-insert":        t.Insert(v, 0),
		"one-redaction":     t.Forget(vs[5000], v),
		"independent-equal": func() Tree[int] { other, _ := benchmarkFixture(10000); return other }(),
	} {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				benchmarkTree = t.Join(other)
			}
		})
	}
}
