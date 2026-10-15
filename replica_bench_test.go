package hearsay

import (
	"bytes"
	"fmt"
	"testing"
)

func benchmarkReplica(b *testing.B, size int) *Rumors[[]byte] {
	b.Helper()
	peer, err := SeedFrom(bytes.NewReader(bytes.Repeat([]byte{1}, 16)), BytesCodec())
	if err != nil {
		b.Fatal(err)
	}
	r, err := peer.IntoRumors()
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := r.Close(); err != nil {
			b.Error(err)
		}
	})
	for range size {
		if _, err := r.Send([]byte("message")); err != nil {
			b.Fatal(err)
		}
	}
	return r
}

var benchmarkSnapshot Snapshot[[]byte]

func BenchmarkReplicaSnapshot(b *testing.B) {
	r := benchmarkReplica(b, 10000)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		benchmarkSnapshot = r.Snapshot()
	}
}

func BenchmarkReplicaSendRedact(b *testing.B) {
	for _, size := range []int{100, 10000} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			r := benchmarkReplica(b, size)
			payload := []byte("message")
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				v, err := r.Send(payload)
				if err != nil {
					b.Fatal(err)
				}
				if err := r.Redact(v); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkObserverIncremental(b *testing.B) {
	for _, causal := range []bool{false, true} {
		b.Run(fmt.Sprint(causal), func(b *testing.B) {
			r := benchmarkReplica(b, 10000)
			var next func() (Message[[]byte], PollState)
			if causal {
				next = r.CausalMessages().TryNext
			} else {
				next = r.UnorderedMessages().TryNext
			}
			for {
				_, state := next()
				if state != Ready {
					break
				}
			}
			payload := []byte("delta")
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				v, err := r.Send(payload)
				if err != nil {
					b.Fatal(err)
				}
				message, state := next()
				if state != Ready || message.Err != nil || !message.Version.Equal(v) {
					b.Fatal("missing delta")
				}
				if _, state := next(); state != Quiet {
					b.Fatal("duplicate delta")
				}
				if err := r.Redact(v); err != nil {
					b.Fatal(err)
				}
				if _, state := next(); state != Quiet {
					b.Fatal("redaction delivered content")
				}
			}
		})
	}
}
