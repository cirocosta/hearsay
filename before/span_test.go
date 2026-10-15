package before

import (
	"errors"
	"testing"
)

func coordinateVersion(x, y uint64) Version {
	return Version{root: branch(leaf(count{small: x}), leaf(count{small: y}))}
}

func TestSpanPlacement(t *testing.T) {
	s, err := NewSpan(coordinateVersion(1, 1), coordinateVersion(3, 3))
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name       string
		x, y       uint64
		want       Placement
		dominance  Dominance
		precedence Precedence
	}{
		{"before", 0, 0, Placement{Relation: PlacementBefore}, DominanceBefore, PrecedenceBefore},
		{"start", 1, 1, Placement{PlacementAt, Start}, DominanceBetween, PrecedenceBefore},
		{
			"between",
			2,
			2,
			Placement{Relation: PlacementBetween},
			DominanceBetween,
			PrecedenceBetween,
		},
		{"end", 3, 3, Placement{PlacementAt, End}, DominanceAfter, PrecedenceBetween},
		{"after", 4, 4, Placement{Relation: PlacementAfter}, DominanceAfter, PrecedenceAfter},
		{
			"concurrent start",
			0,
			2,
			Placement{PlacementConcurrent, Start},
			DominanceBefore,
			PrecedenceBetween,
		},
		{
			"concurrent end",
			2,
			4,
			Placement{PlacementConcurrent, End},
			DominanceBetween,
			PrecedenceAfter,
		},
		{
			"concurrent both",
			0,
			4,
			Placement{PlacementConcurrent, Both},
			DominanceBefore,
			PrecedenceAfter,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v := coordinateVersion(tc.x, tc.y)
			if got := s.Place(v); got != tc.want {
				t.Fatalf("placement %v, want %v", got, tc.want)
			}
			if s.Dominance(v) != tc.dominance || s.Precedence(v) != tc.precedence {
				t.Fatal("directional classification")
			}
		})
	}
	point := Point(s.Lo())
	if point.Place(s.Lo()) != (Placement{PlacementAt, Both}) {
		t.Fatal("point placement")
	}
	if point.Place(coordinateVersion(0, 4)) != (Placement{PlacementConcurrent, Both}) {
		t.Fatal("concurrent point placement")
	}
}

func TestSpanConstructionAndProjection(t *testing.T) {
	a, b := coordinateVersion(2, 0), coordinateVersion(0, 2)
	if _, err := NewSpan(a, b); !errors.Is(err, ErrCrossed) {
		t.Fatal("concurrent bounds accepted")
	}
	if _, err := NewSpan(a, Version{}); !errors.Is(err, ErrCrossed) {
		t.Fatal("reversed bounds accepted")
	}
	s := a.Span(b)
	if !s.Lo().IsEmpty() || !s.Hi().Equal(coordinateVersion(2, 2)) || !s.Contains(a) ||
		!s.Contains(b) {
		t.Fatal("causal hull")
	}
	p := SeedParty()
	q := p.Fork()
	start := Version{}.Tick(p)
	hi := start.Join(Version{}.Tick(q))
	s, err := NewSpan(start, hi)
	if err != nil {
		t.Fatal(err)
	}
	own := s.Project(p)
	if !own.ToSpan().Equal(Point(start)) || own.Place(start) != (Placement{PlacementAt, Both}) ||
		own.Contains(hi) {
		t.Fatal("projected span or probe semantics")
	}
	p.Fork()
	if !own.ToSpan().Equal(Point(start)) {
		t.Fatal("party mutation changed immutable projection view")
	}
}

func TestSpanAlgebra(t *testing.T) {
	var versions []Version
	for x := uint64(0); x < 3; x++ {
		for y := uint64(0); y < 3; y++ {
			versions = append(versions, coordinateVersion(x, y))
		}
	}
	var spans []Span
	for _, lo := range versions {
		for _, hi := range versions {
			if s, err := NewSpan(lo, hi); err == nil {
				spans = append(spans, s)
			}
		}
	}
	for _, a := range spans {
		for _, b := range spans {
			union := a.Union(b)
			intersection, ok := a.Intersect(b)
			if !union.ContainsSpan(a) || !union.ContainsSpan(b) || !union.Equal(b.Union(a)) {
				t.Fatal("span union bounds")
			}
			if !a.Join(b).Equal(b.Join(a)) || !a.Meet(b).Equal(b.Meet(a)) {
				t.Fatal("pointwise commutativity")
			}
			if !a.Join(a.Meet(b)).Equal(a) || !a.Meet(a.Join(b)).Equal(a) {
				t.Fatal("pointwise absorption")
			}
			found := false
			for _, v := range versions {
				want := a.Contains(v) && b.Contains(v)
				found = found || want
				if ok && intersection.Contains(v) != want {
					t.Fatal("intersection differs from exhaustive membership")
				}
			}
			if ok != found {
				t.Fatal("intersection emptiness differs from exhaustive membership")
			}
		}
	}
}

func TestSpanFolds(t *testing.T) {
	a, b, c := Point(
		coordinateVersion(1, 0),
	), Point(
		coordinateVersion(0, 1),
	), Point(
		coordinateVersion(2, 2),
	)
	if !a.UnionAll(b, c).Equal(a.Union(b).Union(c)) || !a.JoinAll(b, c).Equal(a.Join(b).Join(c)) ||
		!a.MeetAll(b, c).Equal(a.Meet(b).Meet(c)) {
		t.Fatal("span folds")
	}
	if _, ok := a.IntersectAll(b, c); ok {
		t.Fatal("empty intersection fold")
	}
	if got, ok := a.IntersectAll(); !ok || !got.Equal(a) {
		t.Fatal("empty operand fold")
	}
}
