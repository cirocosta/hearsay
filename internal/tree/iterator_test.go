package tree

import "testing"

func TestSinceIterator(t *testing.T) {
	vs := versions(100)
	var snapshot Tree[int]
	for i, v := range vs {
		snapshot = snapshot.Insert(v, i)
	}
	it := snapshot.Since(vs[49])
	count := 0
	for {
		v, value, ok := it.Next()
		if !ok {
			break
		}
		if value < 50 || v.LessEqual(vs[49]) {
			t.Fatal("since admitted old entry")
		}
		count++
	}
	if count != 50 {
		t.Fatalf("since returned %d entries", count)
	}
	if _, _, ok := snapshot.Since(snapshot.Latest()).Next(); ok {
		t.Fatal("since latest returned an entry")
	}
}

func TestIteratorSnapshotOrderAndExhaustion(t *testing.T) {
	vs := versions(100)
	var snapshot Tree[int]
	for i, v := range vs {
		snapshot = snapshot.Insert(v, i)
	}
	it := snapshot.Iterator()
	updated := snapshot.Forget(vs[0], snapshot.Latest())
	if updated.Len() != 99 {
		t.Fatal("fixture redaction failed")
	}
	for version, value := range snapshot.All() {
		gotVersion, gotValue, ok := it.Next()
		if !ok || !version.Equal(gotVersion) || value != gotValue {
			t.Fatal("iterator diverged from snapshot order")
		}
	}
	for i := 0; i < 2; i++ {
		if _, _, ok := it.Next(); ok {
			t.Fatal("exhausted iterator yielded entry")
		}
	}
	for _, frame := range it.frames {
		if frame.node != nil {
			t.Fatal("exhausted iterator retained tree")
		}
	}
	if _, _, ok := (Tree[int]{}).Iterator().Next(); ok {
		t.Fatal("empty iterator yielded entry")
	}
}

func TestIteratorMaximumDepth(t *testing.T) {
	v := versions(1)[0]
	root := leaf(Path{}, v, 0, 0)
	for depth := 0; depth < 32; depth++ {
		var p Path
		p[depth] = 1
		root = put(root, leaf(p, v, depth+1, 0), 0)
	}
	it := (Tree[int]{root: root}).Iterator()
	count := 0
	for {
		_, _, ok := it.Next()
		if !ok {
			break
		}
		count++
	}
	if count != 33 {
		t.Fatalf("deep walk returned %d entries", count)
	}
}
