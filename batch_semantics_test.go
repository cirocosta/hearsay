package hearsay

import (
	"encoding/hex"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/cirocosta/hearsay/before"
)

func TestBatchUpstreamPerKeyHistoryFixtures(t *testing.T) {
	encoded, err := os.ReadFile("testdata/batch_semantics.txt")
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(strings.TrimSpace(string(encoded)), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 4 {
			t.Fatalf("malformed oracle line %q", line)
		}
		t.Run(fields[0], func(t *testing.T) {
			replica := runBatchFixture(t, fields[0])
			snapshot := replica.Snapshot()
			if latest := hex.EncodeToString(
				snapshot.Latest().AppendBinary(nil),
			); latest != fields[1] {
				t.Fatalf("frontier %s; Rust %s", latest, fields[1])
			}
			count, err := strconv.Atoi(fields[2])
			if err != nil {
				t.Fatal(err)
			}
			if snapshot.Len() != count {
				t.Fatalf("live count %d; Rust %d", snapshot.Len(), count)
			}
			var versions []string
			for version := range snapshot.Versions() {
				versions = append(versions, hex.EncodeToString(version.AppendBinary(nil)))
			}
			slices.Sort(versions)
			got := strings.Join(versions, ",")
			if len(versions) == 0 {
				got = "-"
			}
			if got != fields[3] {
				t.Fatalf("live versions %s; Rust %s", got, fields[3])
			}
		})
	}
}

func runBatchFixture(t *testing.T, name string) *Rumors[[]byte] {
	t.Helper()
	replica := seededBytes(t)
	fresh := before.Version{}.Tick(before.SeedParty())
	switch name {
	case "duplicate-redact", "duplicate-around-missing":
		version, err := replica.Send([]byte{7})
		if err != nil {
			t.Fatal(err)
		}
		versions := []Version{version, version}
		if name == "duplicate-around-missing" {
			versions = []Version{version, {}, version}
		}
		if err := replica.RedactAll(versions); err != nil {
			t.Fatal(err)
		}
	case "cancel-fresh", "send-after-cancel", "cancel-then-send":
		if err := replica.Batch(func(batch *Batch[[]byte]) error {
			if err := batch.Send([]byte{1}); err != nil {
				return err
			}
			if err := batch.Redact(fresh); err != nil {
				return err
			}
			if name == "cancel-then-send" {
				return batch.Send([]byte{2})
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if name == "send-after-cancel" {
			if _, err := replica.Send([]byte{2}); err != nil {
				t.Fatal(err)
			}
		}
	case "mixed-final-key-stamps":
		version, err := replica.Send([]byte{7})
		if err != nil {
			t.Fatal(err)
		}
		if err := replica.Batch(func(batch *Batch[[]byte]) error {
			if err := batch.RedactAll([]Version{version, version}); err != nil {
				return err
			}
			if err := batch.Send([]byte{9}); err != nil {
				return err
			}
			return batch.Redact(version)
		}); err != nil {
			t.Fatal(err)
		}
	case "missing-only":
		if err := replica.RedactAll([]Version{{}, {}}); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatalf("unknown upstream fixture %q", name)
	}
	return replica
}

func TestBatchCancelledFreshKeyDoesNotWakeChanges(t *testing.T) {
	replica := seededBytes(t)
	changes := replica.Changes()
	if changes.TryNext() != Ready {
		t.Fatal("initial change missing")
	}
	original := replica.Snapshot()
	fresh := Version{}.Tick(before.SeedParty())
	if err := replica.Batch(func(batch *Batch[[]byte]) error {
		if err := batch.Send([]byte("invisible")); err != nil {
			return err
		}
		return batch.Redact(fresh)
	}); err != nil {
		t.Fatal(err)
	}
	if !original.Equal(replica.Snapshot()) || changes.TryNext() != Quiet {
		t.Fatal("cancelled fresh key published content or history")
	}
}
