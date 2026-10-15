package before

import (
	"math/big"
	"testing"
)

func TestMinTicksCountsSeparatedPeaks(t *testing.T) {
	p := SeedParty()
	q := p.Fork()
	p.Fork()
	r := q.Fork()
	v := Version{}.Tick(p).Tick(r)
	if got := v.MinTicks(); got.Cmp(big.NewInt(2)) != 0 {
		t.Fatalf("minimum events %s; want 2", got)
	}
	if v.Minimum().Sign() != 0 || v.Maximum().Cmp(big.NewInt(1)) != 0 {
		t.Fatal("fixture does not distinguish event count from height bounds")
	}
}

func TestMinTicksSubtractsSharedHistoryAtWideCounts(t *testing.T) {
	p := SeedParty()
	n := new(big.Int).Lsh(big.NewInt(1), 130)
	v := Version{}.TicksBig(p, n)
	q := p.Fork()
	v = v.Tick(p).Tick(q)
	want := new(big.Int).Add(n, big.NewInt(1))
	if got := v.MinTicks(); got.Cmp(want) != 0 {
		t.Fatalf("minimum events %s; want %s", got, want)
	}
	n.Add(n, big.NewInt(1))
	if v.MinTicks().Cmp(want) != 0 {
		t.Fatal("measurement aliases caller storage")
	}
}
