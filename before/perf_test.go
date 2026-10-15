package before

import (
	"fmt"
	"testing"
)

func unaryTickFixture(depth int) (*Party, Version) {
	p := SeedParty()
	for range depth {
		p.Fork()
	}
	return p, (Version{}).Ticks(p, 10)
}

func fragmentedTickFixture(regions int) (*Party, Version) {
	parties := []*Party{SeedParty()}
	for len(parties) < 2*regions {
		next := make([]*Party, 0, 2*len(parties))
		for _, p := range parties {
			q := p.Fork()
			next = append(next, p, q)
		}
		parties = next
	}
	var history Version
	for i, p := range parties {
		history = history.Join((Version{}).Ticks(p, uint64(i%7+1)))
	}
	owner := parties[0]
	for i := 2; i < len(parties); i += 2 {
		if err := owner.Join(parties[i]); err != nil {
			panic(err)
		}
	}
	return owner, history.Tick(owner)
}

func BenchmarkTickTopology(b *testing.B) {
	for _, depth := range []int{1, 8, 64, 256} {
		b.Run(fmt.Sprintf("unary%d", depth), func(b *testing.B) {
			p, v := unaryTickFixture(depth)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				v = v.Tick(p)
			}
			versionSink = v
		})
	}
	for _, regions := range []int{16, 128} {
		b.Run(fmt.Sprintf("fragmented%d", regions), func(b *testing.B) {
			p, v := fragmentedTickFixture(regions)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				v = v.Tick(p)
			}
			versionSink = v
		})
	}
}

func TestPerfFixtureCausality(t *testing.T) {
	for _, depth := range []int{1, 8, 64, 256} {
		p, v := unaryTickFixture(depth)
		checkTickFixture(t, p, v, 1)
	}
	for _, regions := range []int{16, 128} {
		p, v := fragmentedTickFixture(regions)
		checkTickFixture(t, p, v, regions)
	}
}

func ownedRegions(p *identity) int {
	if p == nil {
		return 0
	}
	if p == owned {
		return 1
	}
	return ownedRegions(p.left) + ownedRegions(p.right)
}

func checkTickFixture(t *testing.T, p *Party, v Version, regions int) {
	t.Helper()
	if got := ownedRegions(p.root); got != regions {
		t.Fatalf("fixture owns %d regions, want %d", got, regions)
	}
	for range 3 {
		next := v.Tick(p)
		if v.Compare(next) != Before {
			t.Fatal("fixture tick did not causally advance")
		}
		v = next
	}
}
