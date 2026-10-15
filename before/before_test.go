package before

import (
	"bufio"
	"encoding/hex"
	"errors"
	"math/big"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
	"testing"
)

func TestClockLifecycle(t *testing.T) {
	a := Seed()
	b := a.Fork()
	initial := a.Version()
	av, bv := a.Tick(), b.Tick()
	if !av.Concurrent(bv) {
		t.Fatal("independent events must be concurrent")
	}
	if !initial.IsEmpty() {
		t.Fatal("tick mutated snapshot")
	}
	merged := b.Absorb(av)
	if merged.Compare(av) != After || merged.Compare(bv) != After {
		t.Fatal("absorb did not merge both histories")
	}
	if err := a.Join(b); err != nil {
		t.Fatal(err)
	}
	if !a.Party().IsSeed() || !a.Version().Equal(merged) {
		t.Fatal("join did not recycle identity and history")
	}
	defer func() {
		if r := recover(); !errors.Is(asError(r), ErrRetired) {
			t.Fatalf("retired donor panic = %v", r)
		}
	}()
	b.Tick()
}
func asError(v any) error { e, _ := v.(error); return e }

func TestOverlapIsTransactional(t *testing.T) {
	a, b := Seed(), Seed()
	a.Tick()
	old := a.Version()
	if err := a.Join(b); !errors.Is(err, ErrOverlap) {
		t.Fatal(err)
	}
	if !a.Version().Equal(old) || !b.Party().IsSeed() {
		t.Fatal("overlap changed participants")
	}
	if err := a.Sync(b); !errors.Is(err, ErrOverlap) {
		t.Fatal(err)
	}
}

func TestBulkTicksAndWideCounts(t *testing.T) {
	rng := rand.New(rand.NewPCG(17, 31))
	parties := []*Party{SeedParty()}
	v := Version{}
	for step := 0; step < 200; step++ {
		p := parties[rng.IntN(len(parties))]
		if len(parties) < 24 && rng.IntN(4) == 0 {
			parties = append(parties, p.Fork())
		}
		for _, p := range parties {
			n := uint64(rng.IntN(12))
			want := v
			for range n {
				want = want.Tick(p)
			}
			if got := v.Ticks(p, n); !got.Equal(want) {
				t.Fatalf("bulk differs at step %d count %d", step, n)
			}
		}
		v = v.Tick(p)
	}
	p := SeedParty()
	huge := new(big.Int).Lsh(big.NewInt(1), 256)
	w := Version{}.TicksBig(p, huge)
	if w.Minimum().Cmp(huge) != 0 {
		t.Fatal("wide count lost precision")
	}
	w = w.Tick(p)
	huge.Add(huge, big.NewInt(1))
	if w.Maximum().Cmp(huge) != 0 {
		t.Fatal("wide increment lost precision")
	}
	u := Version{}.Ticks(p, ^uint64(0)).Tick(p)
	if u.Minimum().BitLen() != 65 {
		t.Fatal("uint64 overflow did not promote")
	}
}

func TestPartyConservation(t *testing.T) {
	rng := rand.New(rand.NewPCG(2, 7))
	parties := []*Party{SeedParty()}
	for step := 0; step < 500; step++ {
		i := rng.IntN(len(parties))
		if len(parties) == 1 || len(parties) < 32 && rng.IntN(2) == 0 {
			parties = append(parties, parties[i].Fork())
		} else {
			j := (i + 1 + rng.IntN(len(parties)-1)) % len(parties)
			if err := parties[i].Join(parties[j]); err != nil {
				t.Fatal(err)
			}
			parties = append(parties[:j], parties[j+1:]...)
		}
		all := (*identity)(nil)
		for i, p := range parties {
			for _, q := range parties[:i] {
				if !p.IsDisjoint(q) {
					t.Fatalf("overlap at step %d", step)
				}
			}
			all = unionIdentity(all, p.root)
		}
		if all != owned {
			t.Fatalf("lost identity at step %d", step)
		}
	}
}

func generatedVersions() []Version {
	rng := rand.New(rand.NewPCG(55, 23))
	clocks := []*Clock{Seed()}
	versions := []Version{{}}
	for step := 0; step < 100; step++ {
		c := clocks[rng.IntN(len(clocks))]
		switch rng.IntN(4) {
		case 0:
			if len(clocks) < 16 {
				clocks = append(clocks, c.Fork())
			}
		case 1:
			c.Absorb(clocks[rng.IntN(len(clocks))].Version())
		default:
			c.Tick()
		}
		versions = append(versions, c.Version())
	}
	return versions
}

func TestVersionLatticeLaws(t *testing.T) {
	versions := generatedVersions()
	for i, a := range versions {
		if !a.Join(a).Equal(a) || !a.Meet(a).Equal(a) {
			t.Fatal("idempotence")
		}
		for j, b := range versions {
			join, meet := a.Join(b), a.Meet(b)
			if !join.Equal(b.Join(a)) || !meet.Equal(b.Meet(a)) {
				t.Fatal("commutativity")
			}
			if !a.LessEqual(join) || !b.LessEqual(join) || !meet.LessEqual(a) ||
				!meet.LessEqual(b) {
				t.Fatal("bounds")
			}
			if !a.Join(meet).Equal(a) || !a.Meet(join).Equal(a) {
				t.Fatal("absorption")
			}
			if a.LessEqual(b) != join.Equal(b) {
				t.Fatal("order and join disagree")
			}
			c := versions[(i*17+j*13)%len(versions)]
			if !a.Join(b).Join(c).Equal(a.Join(b.Join(c))) ||
				!a.Meet(b).Meet(c).Equal(a.Meet(b.Meet(c))) {
				t.Fatal("associativity")
			}
			if !a.Meet(b.Join(c)).Equal(a.Meet(b).Join(a.Meet(c))) {
				t.Fatal("distributivity")
			}
		}
	}
}

func TestTickLocalityAndFill(t *testing.T) {
	p := SeedParty()
	q := p.Fork()
	v := Version{}.Ticks(p, 8).Ticks(q, 3)
	beforeQ := v.Project(q)
	w := v.Tick(p)
	if !v.LessEqual(w) || v.Equal(w) || !beforeQ.Equal(w.Project(q)) {
		t.Fatal("tick must increase only owned history")
	}
	if err := p.Join(q); err != nil {
		t.Fatal(err)
	}
	maximum := v.Maximum()
	w = v.Tick(p)
	if w.node().left != nil || w.Minimum().Cmp(maximum) != 0 {
		t.Fatal("fill did not collapse to maximum")
	}
	maximum.Add(maximum, big.NewInt(1))
	if w.Tick(p).Maximum().Cmp(maximum) != 0 {
		t.Fatal("grow after fill")
	}
}

func TestFromIntoParts(t *testing.T) {
	c := Seed()
	c.Tick()
	p, v := c.IntoParts()
	restored := FromParts(p, v)
	if !restored.Party().IsSeed() || restored.Version().Maximum().Uint64() != 1 {
		t.Fatal("parts round trip")
	}
}

func expandEvent(e *event, depth int, out *[]count) {
	if depth == 0 {
		if e.left != nil {
			panic("oracle depth too small")
		}
		*out = append(*out, e.low)
		return
	}
	l, r := children(e)
	expandEvent(l, depth-1, out)
	expandEvent(r, depth-1, out)
}

func TestDenseCausalOracle(t *testing.T) {
	versions := generatedVersions()
	for _, a := range versions {
		var denseA []count
		expandEvent(a.node(), 12, &denseA)
		for _, b := range versions {
			var denseB, denseJoin, denseMeet []count
			expandEvent(b.node(), 12, &denseB)
			expandEvent(a.Join(b).node(), 12, &denseJoin)
			expandEvent(a.Meet(b).node(), 12, &denseMeet)
			less := true
			for i, x := range denseA {
				y := denseB[i]
				less = less && x.cmp(y) <= 0
				if denseJoin[i].cmp(maxCount(x, y)) != 0 || denseMeet[i].cmp(minCount(x, y)) != 0 {
					t.Fatal("lattice differs from pointwise oracle")
				}
			}
			if a.LessEqual(b) != less {
				t.Fatal("order differs from pointwise oracle")
			}
		}
	}
}

func TestRankExtendsCausality(t *testing.T) {
	versions := generatedVersions()
	for _, a := range versions {
		var dense []count
		expandEvent(a.node(), 12, &dense)
		area := new(big.Int)
		for _, n := range dense {
			area.Add(area, n.big())
		}
		want := new(big.Rat).SetFrac(area, big.NewInt(int64(len(dense))))
		if a.Rank().Cmp(want) != 0 {
			t.Fatal("rank differs from dense integral")
		}
		for _, b := range versions {
			c := a.RankedCompare(b)
			if c != -b.RankedCompare(a) {
				t.Fatal("ranked antisymmetry")
			}
			if a.Compare(b) == Before && (c >= 0 || a.Rank().Cmp(b.Rank()) >= 0) {
				t.Fatal("rank failed to extend causal order")
			}
			if (c == 0) != a.Equal(b) {
				t.Fatal("ranked equality")
			}
		}
	}
}

func TestDeepDecodedAtoms(t *testing.T) {
	const depth = 4096
	i := owned
	for range depth {
		i = identityBranch(i, nil)
	}
	encoded, err := (&Party{root: i}).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	p, err := DecodeParty(encoded)
	if err != nil {
		t.Fatal(err)
	}
	q := p.Fork()
	if !p.IsDisjoint(q) {
		t.Fatal("deep fork overlaps")
	}
	a, b := Version{}.Tick(p), Version{}.Tick(q)
	if !a.Concurrent(b) {
		t.Fatal("deep independent ticks not concurrent")
	}
	joined := a.Join(b)
	if !a.LessEqual(joined) || !b.LessEqual(joined) || !a.Meet(b).IsEmpty() {
		t.Fatal("deep lattice")
	}
	decoded, err := DecodeVersion(joined.AppendBinary(nil))
	if err != nil {
		t.Fatal(err)
	}
	if !decoded.Equal(joined) || !decoded.Project(p).Equal(a) {
		t.Fatal("deep version round trip")
	}
	if err := p.Join(q); err != nil {
		t.Fatal(err)
	}
	if !p.Covers(&Party{root: i}) {
		t.Fatal("deep join lost ownership")
	}
	want := new(big.Rat).SetFrac(big.NewInt(1), new(big.Int).Lsh(big.NewInt(1), depth))
	if joined.Rank().Cmp(want) != 0 {
		t.Fatal("deep exact rank")
	}
	if joined.Tick(p).Compare(joined) != After {
		t.Fatal("deep tick failed to advance")
	}
}

func TestUpstreamClockTrace(t *testing.T) {
	f, err := os.Open("testdata/clock_trace.txt")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := f.Close(); err != nil {
			t.Error(err)
		}
	})
	clocks := []*Clock{Seed()}
	scanner := bufio.NewScanner(f)
	line := 0
	for scanner.Scan() {
		line++
		fields := strings.Fields(scanner.Text())
		var got []*Clock
		var want []string
		clocks, got, want = applyTraceStep(t, clocks, fields)
		checkTraceClocks(t, line, fields[0], got, want)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if line != 300 {
		t.Fatalf("trace contains %d operations", line)
	}
}

func traceIndex(t *testing.T, s string) int {
	t.Helper()
	n, err := strconv.Atoi(s)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func applyTraceStep(t *testing.T, clocks []*Clock, fields []string) ([]*Clock, []*Clock, []string) {
	t.Helper()
	i := traceIndex(t, fields[1])
	switch fields[0] {
	case "fork":
		c := clocks[i].Fork()
		return append(clocks, c), []*Clock{clocks[i], c}, fields[2:]
	case "tick":
		clocks[i].Tick()
	case "absorb":
		j := traceIndex(t, fields[2])
		clocks[i].Absorb(clocks[j].Version())
		return clocks, []*Clock{clocks[i]}, fields[3:]
	case "join":
		donor := clocks[len(clocks)-1]
		clocks = clocks[:len(clocks)-1]
		if err := clocks[i].Join(donor); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatalf("unknown trace operation: %s", fields[0])
	}
	return clocks, []*Clock{clocks[i]}, fields[2:]
}

func checkTraceClocks(t *testing.T, line int, operation string, got []*Clock, want []string) {
	t.Helper()
	for j, c := range got {
		encoded, err := c.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		if hex.EncodeToString(encoded) != want[j] {
			t.Fatalf(
				"upstream divergence at line %d: %s: got %x, want %s",
				line,
				operation,
				encoded,
				want[j],
			)
		}
	}
}
