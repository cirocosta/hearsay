package before

import "testing"

var (
	placementSink Placement
	spanSink      Span
)

func BenchmarkSpanPlace(b *testing.B) {
	versions := generatedVersions()
	s := versions[60].Span(versions[99])
	probe := versions[83]
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		placementSink = s.Place(probe)
	}
}

func BenchmarkSpanIntersect(b *testing.B) {
	versions := generatedVersions()
	a, c := versions[60].Span(versions[99]), versions[40].Span(versions[80])
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		spanSink, _ = a.Intersect(c)
	}
}
