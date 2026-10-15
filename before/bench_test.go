package before

import "testing"

var versionSink Version

func BenchmarkTickSeed(b *testing.B) {
	p := SeedParty()
	v := Version{}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		v = v.Tick(p)
	}
	versionSink = v
}

func BenchmarkTickForked(b *testing.B) {
	p := SeedParty()
	for range 8 {
		p.Fork()
	}
	v := Version{}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		v = v.Tick(p)
	}
	versionSink = v
}

func BenchmarkVersionJoin(b *testing.B) {
	versions := generatedVersions()
	a, c := versions[83], versions[99]
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		versionSink = a.Join(c)
	}
}

func BenchmarkVersionCompare(b *testing.B) {
	versions := generatedVersions()
	a, c := versions[83], versions[99]
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_ = a.Compare(c)
	}
}

func BenchmarkPartyForkJoin(b *testing.B) {
	p := SeedParty()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		q := p.Fork()
		if err := p.Join(q); err != nil {
			b.Fatal(err)
		}
	}
}
