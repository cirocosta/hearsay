package testlink

import (
	"math"
	"math/bits"
	"sync"
	"time"
)

type direction struct {
	mu          sync.Mutex
	next        time.Time
	rate        uint64
	propagation time.Duration
}

func (d *direction) reserve(now time.Time, size int) time.Time {
	d.mu.Lock()
	defer d.mu.Unlock()
	start := now
	if d.next.After(start) {
		start = d.next
	}
	d.next = start.Add(serialization(size, d.rate))
	return d.next.Add(d.propagation)
}

func serialization(size int, rate uint64) time.Duration {
	if rate == 0 || size == 0 {
		return 0
	}
	count := uint64(size)
	seconds, remainder := count/rate, count%rate
	if seconds > math.MaxInt64/uint64(time.Second) {
		return time.Duration(math.MaxInt64)
	}
	hi, lo := bits.Mul64(remainder, uint64(time.Second))
	fraction, rest := bits.Div64(hi, lo, rate)
	if rest != 0 {
		fraction++
	}
	whole := seconds * uint64(time.Second)
	if fraction > math.MaxInt64-whole {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(whole + fraction)
}
