package reconcile

import (
	"context"
	"fmt"
	"testing"

	"github.com/cirocosta/hearsay/before"
	"github.com/cirocosta/hearsay/internal/cbor"
	"github.com/cirocosta/hearsay/internal/protocol"
	"github.com/cirocosta/hearsay/internal/tree"
	"github.com/cirocosta/hearsay/link"
)

type benchFixture struct {
	left, right   tree.Tree[[]byte]
	local, remote protocol.Greeting
	want          tree.Hash
	count         int
}

func reconciliationFixture(size, delta, payloadBytes int, redactions bool) benchFixture {
	clock := before.Seed()
	payload := cbor.AppendHead(nil, cbor.MajorBytes, uint64(payloadBytes))
	payload = append(payload, make([]byte, payloadBytes)...)
	var base tree.Tree[[]byte]
	versions := make([]before.Version, 0, delta)
	for i := 0; i < size; i++ {
		version := clock.Tick()
		base = base.Insert(version, payload)
		if len(versions) < delta {
			versions = append(versions, version)
		}
	}
	right := base
	for i := 0; i < delta; i++ {
		if redactions {
			right = right.Forget(versions[i], clock.Tick())
		} else {
			right = right.Insert(clock.Tick(), payload)
		}
	}
	want := base.Join(right)
	return benchFixture{
		left:   base,
		right:  right,
		local:  greeting(base, protocol.DefaultTargetMessageSize),
		remote: greeting(right, protocol.DefaultTargetMessageSize),
		want:   want.Hash(),
		count:  want.Len(),
	}
}

func BenchmarkRun(b *testing.B) {
	for _, size := range []int{100, 10_000} {
		for _, payload := range []int{32, 1024} {
			for _, redactions := range []bool{false, true} {
				for _, delta := range []int{0, 1, 100} {
					if redactions && delta == 0 {
						continue
					}
					kind := "sends"
					if redactions {
						kind = "redactions"
					}
					fixture := reconciliationFixture(size, delta, payload, redactions)
					for _, budget := range []uint64{0, 512 << 20} {
						name := fmt.Sprintf(
							"N%d/D%d/%s/payload%d/budget%d",
							size,
							delta,
							kind,
							payload,
							budget,
						)
						b.Run(name, func(b *testing.B) { benchmarkFixture(b, fixture, budget) })
					}
				}
			}
		}
	}
}

func benchmarkFixture(b *testing.B, fixture benchFixture, budget uint64) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	var bytesSent, opened uint64
	for range b.N {
		a, c := benchmarkPair(b, fixture, budget)
		bytesSent += a.Stats.BytesSent + c.Stats.BytesSent
		opened += a.Stats.StreamsOpened + c.Stats.StreamsOpened
	}
	b.StopTimer()
	b.ReportMetric(float64(bytesSent)/float64(b.N), "wire-B/op")
	b.ReportMetric(float64(opened)/float64(b.N), "streams/op")
}

func benchmarkPair(b *testing.B, fixture benchFixture, budget uint64) (Result, Result) {
	b.Helper()
	left, right := link.Memory()
	type outcome struct {
		result Result
		err    error
	}
	joined := make(chan outcome, 1)
	go func() {
		result, err := Run(
			context.Background(),
			left,
			0,
			fixture.left,
			fixture.local,
			fixture.remote,
			Options{MemoryBudget: budget},
		)
		joined <- outcome{result, err}
	}()
	rightResult, rightErr := Run(
		context.Background(),
		right,
		0,
		fixture.right,
		fixture.remote,
		fixture.local,
		Options{MemoryBudget: budget},
	)
	leftResult := <-joined
	_ = left.Close()
	if leftResult.err != nil || rightErr != nil {
		b.Fatalf("session: left %v right %v", leftResult.err, rightErr)
	}
	if leftResult.result.Tree.Hash() != fixture.want || rightResult.Tree.Hash() != fixture.want ||
		leftResult.result.Tree.Len() != fixture.count ||
		rightResult.Tree.Len() != fixture.count {
		b.Fatal("session differs from causal reconciliation oracle")
	}
	return leftResult.result, rightResult
}
