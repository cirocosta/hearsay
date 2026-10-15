package before

import "testing"

func oracleTickPair(x, y uint64, party int) (uint64, uint64) {
	switch party {
	case 0:
		if x != y {
			return max(x, y), max(x, y)
		}
		return x + 1, y + 1
	case 1:
		if x < y {
			return y, y
		}
		return x + 1, y
	default:
		if y < x {
			return x, x
		}
		return x, y + 1
	}
}

func TestFillChangeIsOneEvent(t *testing.T) {
	whole := SeedParty()
	left := SeedParty()
	right := left.Fork()
	parties := []*Party{whole, left, right}
	for x := uint64(0); x < 6; x++ {
		for y := uint64(0); y < 6; y++ {
			for kind, p := range parties {
				initial := coordinateVersion(x, y)
				wantX, wantY := x, y
				for count := uint64(0); count < 9; count++ {
					want := coordinateVersion(wantX, wantY)
					if got := initial.Ticks(p, count); !got.Equal(want) {
						t.Fatalf(
							"fill/grow threshold: initial(%d,%d),party%d,count%d: got%x want%x",
							x,
							y,
							kind,
							count,
							got.AppendBinary(nil),
							want.AppendBinary(nil),
						)
					}
					wantX, wantY = oracleTickPair(wantX, wantY, kind)
				}
				if !initial.Equal(coordinateVersion(x, y)) {
					t.Fatal("fill mutated an immutable input")
				}
			}
		}
	}
}
