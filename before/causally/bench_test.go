package causally

import "testing"

var (
	coverageSink   Coverage
	membershipSink bool
)

func BenchmarkCoverage(b *testing.B) {
	v := gridVersions()
	q := Delta(v[1], v[8])
	span := v[0].Span(v[7])
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		coverageSink = q.Coverage(span)
	}
}

func BenchmarkContains(b *testing.B) {
	v := gridVersions()
	q := Delta(v[1], v[8])
	probe := v[5]
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		membershipSink = q.Contains(probe)
	}
}
