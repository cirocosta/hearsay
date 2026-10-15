package testlink

import (
	"math"
	"math/big"
	"testing"
	"time"
)

func TestSerializationAgainstRationalOracle(t *testing.T) {
	for _, size := range []int{0, 1, 7, 1000, 65536, math.MaxInt} {
		for _, rate := range []uint64{0, 1, 3, 1000, 1 << 30, math.MaxUint64} {
			want := new(big.Int)
			if rate != 0 {
				numerator := new(
					big.Int,
				).Mul(big.NewInt(int64(size)), big.NewInt(int64(time.Second)))
				divisor := new(big.Int).SetUint64(rate)
				remainder := new(big.Int)
				want.QuoRem(numerator, divisor, remainder)
				if remainder.Sign() > 0 {
					want.Add(want, big.NewInt(1))
				}
				if want.Cmp(big.NewInt(math.MaxInt64)) > 0 {
					want.SetInt64(math.MaxInt64)
				}
			}
			if got := serialization(size, rate); int64(got) != want.Int64() {
				t.Fatalf("size%d rate%d: got%d want%s", size, rate, got, want)
			}
		}
	}
}

func TestAbsolutePacketSchedule(t *testing.T) {
	now := time.Unix(1, 0)
	d := direction{rate: 1000, propagation: 50 * time.Millisecond}
	// Three simultaneously accepted packets form one serialized train. The
	// propagation term is measured from each serialization endpoint; it is
	// never included in the reservation cursor for the next packet.
	for i := 1; i <= 3; i++ {
		got := d.reserve(now, 10)
		want := now.Add(time.Duration(i)*10*time.Millisecond + 50*time.Millisecond)
		if !got.Equal(want) {
			t.Fatalf("packet%d: got%s want%s", i, got, want)
		}
	}
	later := now.Add(time.Second)
	if got := d.reserve(later, 10); !got.Equal(later.Add(60 * time.Millisecond)) {
		t.Fatal("idle serializer did not restart at enqueue time", got)
	}
}

func TestDirectionalReservationsAreIndependent(t *testing.T) {
	now := time.Unix(1, 0)
	directions := [2]direction{
		{rate: 1000, propagation: time.Second},
		{rate: 1000, propagation: time.Second},
	}
	for range 10 {
		_ = directions[0].reserve(now, 1000)
	}
	if got := directions[1].reserve(now, 1); !got.Equal(now.Add(time.Second + time.Millisecond)) {
		t.Fatal("reverse direction inherited forward serialization", got)
	}
}
