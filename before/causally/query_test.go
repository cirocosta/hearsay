package causally

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"testing"

	"github.com/cirocosta/hearsay/before"
)

func gridVersions() []before.Version {
	p := before.SeedParty()
	q := p.Fork()
	var versions []before.Version
	for x := uint64(0); x < 3; x++ {
		for y := uint64(0); y < 3; y++ {
			versions = append(
				versions,
				(before.Version{}).Ticks(p, x).Join((before.Version{}).Ticks(q, y)),
			)
		}
	}
	return versions
}

func TestAtomicQueries(t *testing.T) {
	versions := gridVersions()
	for _, bound := range versions {
		for _, v := range versions {
			order := v.Compare(bound)
			checks := []struct {
				query Predicate
				want  bool
			}{
				{All(), true},
				{After(bound), bound.LessEqual(v)},
				{Before(bound), v.LessEqual(bound)},
				{StrictlyAfter(bound), order == before.After},
				{StrictlyBefore(bound), order == before.Before},
				{Since(bound), !v.LessEqual(bound)},
				{Until(bound), !bound.LessEqual(v)},
				{After(bound).Not(), !After(bound).Contains(v)},
				{Before(bound).Not(), !Before(bound).Contains(v)},
				{After(bound).OrConcurrent(), order != before.Before},
				{Before(bound).OrConcurrent(), order != before.After},
			}
			for _, check := range checks {
				if check.query.Contains(v) != check.want {
					t.Fatalf(
						"atomic membership: v %x, bound %x",
						v.AppendBinary(nil),
						bound.AppendBinary(nil),
					)
				}
			}
		}
	}
}

func TestQueryConjunctionAndPolarity(t *testing.T) {
	versions := gridVersions()
	for _, a := range versions {
		for _, b := range versions {
			q, err := After(a).And(Before(b))
			if err != nil {
				t.Fatal(err)
			}
			for _, v := range versions {
				if q.Contains(v) != (a.LessEqual(v) && v.LessEqual(b)) {
					t.Fatal("inclusive conjunction")
				}
				if Delta(a, b).Contains(v) != (!v.LessEqual(a) && v.LessEqual(b)) {
					t.Fatal("delta membership")
				}
				if Toward(a, b).Contains(v) != (a.LessEqual(v) && !b.LessEqual(v)) {
					t.Fatal("toward membership")
				}
			}
			if _, err := Since(a).And(Until(b)); !errors.Is(err, ErrPolarity) {
				t.Fatal("mixed polarity accepted")
			}
		}
	}
}

func assertCoverage(t *testing.T, p Predicate, s before.Span, versions []before.Version) {
	t.Helper()
	admitted, rejected := false, false
	for _, v := range versions {
		if s.Contains(v) {
			if p.Contains(v) {
				admitted = true
			} else {
				rejected = true
			}
		}
	}
	want := Partial
	if !admitted {
		want = Empty
	} else if !rejected {
		want = Full
	}
	if got := p.Coverage(s); got != want {
		t.Fatalf(
			"coverage %v want %v over [%x,%x]",
			got,
			want,
			s.Lo().AppendBinary(nil),
			s.Hi().AppendBinary(nil),
		)
	}
}

func TestQueryCoverageAgainstExhaustiveLattice(t *testing.T) {
	versions := gridVersions()
	var spans []before.Span
	var oracle []byte
	for _, lo := range versions {
		for _, hi := range versions {
			if s, err := before.NewSpan(lo, hi); err == nil {
				spans = append(spans, s)
			}
		}
	}
	for _, a := range versions {
		for _, b := range versions {
			bounded, err := After(a).And(Before(b))
			if err != nil {
				t.Fatal(err)
			}
			downward, err := Since(a).And(Since(b))
			if err != nil {
				t.Fatal(err)
			}
			upward, err := Until(a).And(Until(b))
			if err != nil {
				t.Fatal(err)
			}
			queries := []Predicate{
				All(),
				After(a),
				Before(a),
				StrictlyAfter(a),
				StrictlyBefore(a),
				Since(a),
				Until(a),
				After(a).OrConcurrent(),
				Before(a).OrConcurrent(),
				Delta(a, b),
				Toward(a, b),
				bounded,
				downward,
				upward,
			}
			for _, q := range queries {
				oracle = appendQueryOracle(oracle, q, versions, spans)
				for _, s := range spans {
					assertCoverage(t, q, s, versions)
				}
			}
		}
	}
	const upstream = "ad85933cff1c00fd20794c376e35ec15479456f7a45b289ee9176e56582d5554"
	if got := fmt.Sprintf("%x", sha256.Sum256(oracle)); got != upstream {
		t.Fatalf("Rust query oracle digest %s, want %s", got, upstream)
	}
}

func appendQueryOracle(
	dst []byte,
	q Predicate,
	versions []before.Version,
	spans []before.Span,
) []byte {
	for _, v := range versions {
		value := byte(0)
		if q.Contains(v) {
			value = 1
		}
		dst = append(dst, value)
	}
	for _, s := range spans {
		dst = append(dst, byte(q.Coverage(s)))
	}
	return dst
}

func TestQueryCompositionDoesNotMutateOperands(t *testing.T) {
	v := gridVersions()
	original := Since(v[1])
	first, err := original.And(Since(v[3]))
	if err != nil {
		t.Fatal(err)
	}
	_, err = original.And(Since(v[8]))
	if err != nil {
		t.Fatal(err)
	}
	if len(original.holes) != 1 || len(first.holes) != 2 {
		t.Fatal("query append mutated an operand")
	}
	for _, probe := range v {
		if original.Contains(probe) != Since(v[1]).Contains(probe) {
			t.Fatal("query operand changed")
		}
	}
}

func TestQueryConversions(t *testing.T) {
	versions := gridVersions()
	for _, lo := range versions {
		for _, hi := range versions {
			s := lo.Span(hi)
			q := FromSpan(s)
			for _, v := range versions {
				if q.Contains(v) != s.Contains(v) {
					t.Fatal("span query conversion")
				}
				if FromVersion(lo).Contains(v) != lo.Equal(v) {
					t.Fatal("point query conversion")
				}
			}
		}
	}
}
