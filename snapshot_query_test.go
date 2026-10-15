package hearsay

import (
	"testing"

	"github.com/cirocosta/hearsay/before"
	"github.com/cirocosta/hearsay/before/causally"
	"github.com/cirocosta/hearsay/internal/tree"
)

func TestSnapshotQueryMatchesLeafPredicate(t *testing.T) {
	a := before.Seed()
	b := a.Fork()
	codec := BytesCodec()
	var content tree.Tree[[]byte]
	var versions []Version
	for i := range 40 {
		var version Version
		if i%2 == 0 {
			version = a.Tick()
		} else {
			version = b.Tick()
		}
		if i%7 == 0 {
			a.Absorb(b.Version())
		}
		versions = append(versions, version)
		payload, err := codec.encode([]byte{byte(i)})
		if err != nil {
			t.Fatal(err)
		}
		content = content.Insert(version, payload)
	}
	snapshot := Snapshot[[]byte]{tree: content, codec: codec}
	for _, query := range []causally.Predicate{
		causally.All(), causally.Since(versions[10]), causally.Until(versions[20]),
		causally.After(versions[10]), causally.Before(versions[20]),
		causally.StrictlyAfter(versions[10]), causally.StrictlyBefore(versions[20]),
		causally.Delta(versions[10], versions[30]), causally.Toward(versions[10], versions[30]),
	} {
		want := make(map[string]bool)
		for message := range snapshot.Range(query.Contains) {
			want[string(message.Version.AppendBinary(nil))] = true
		}
		got := make(map[string]bool)
		for message := range snapshot.Query(query) {
			if message.Err != nil {
				t.Fatal(message.Err)
			}
			key := string(message.Version.AppendBinary(nil))
			if !want[key] || got[key] {
				t.Fatal("query yielded unexpected or duplicate version")
			}
			got[key] = true
		}
		if len(got) != len(want) {
			t.Fatalf("got %d; want %d", len(got), len(want))
		}
		count := 0
		for range snapshot.Query(query) {
			count++
			break
		}
		if len(want) > 0 && count != 1 {
			t.Fatal("early stop lost first result")
		}
	}
}
