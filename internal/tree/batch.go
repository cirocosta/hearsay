package tree

import (
	"bytes"
	"slices"

	"github.com/cirocosta/hearsay/before"
)

// Edit is one final action at a version address. Inserts retain immutable Value;
// deletions use Stamp as their causal action timestamp. Callers must collapse
// multiple actions at the same address before Apply, retaining the final stamp.
type Edit[T any] struct {
	Version before.Version
	Stamp   before.Version
	Value   T
	Insert  bool
}

type preparedEdit[T any] struct {
	Edit[T]
	path Path
}

// Apply applies distinct final effects in one persistent traversal. It retains
// untouched nodes and adds history only for keys initially or finally present.
// Duplicate addresses panic before any tree is published.
func (t Tree[T]) Apply(edits []Edit[T]) Tree[T] {
	if len(edits) == 0 {
		return t
	}
	prepared := make([]preparedEdit[T], len(edits))
	for i, edit := range edits {
		prepared[i] = preparedEdit[T]{Edit: edit, path: Address(edit.Version)}
	}
	slices.SortFunc(
		prepared,
		func(a, b preparedEdit[T]) int { return bytes.Compare(a.path[:], b.path[:]) },
	)
	for i := 1; i < len(prepared); i++ {
		if prepared[i-1].path == prepared[i].path {
			panic("tree: duplicate batch address")
		}
	}
	history := t.ceiling
	t.root = applyEdits(t.root, prepared, 0, &history)
	t.ceiling = history
	return t
}

func applyEdits[T any](
	n *node[T],
	edits []preparedEdit[T],
	base uint8,
	history *before.Version,
) *node[T] {
	if n == nil {
		return buildInserted(edits, base, history)
	}
	if len(edits) == 1 {
		return applyEdit(n, edits[0], base, history)
	}
	depth := common(edits[0].path, edits[len(edits)-1].path, base, 32)
	depth = common(n.path, edits[0].path, base, min(n.depth, depth))
	return applyFan(n, edits, base, depth, history)
}

func buildInserted[T any](edits []preparedEdit[T], base uint8, history *before.Version) *node[T] {
	nodes := make([]*node[T], 0, len(edits))
	for _, edit := range edits {
		if edit.Insert {
			nodes = append(nodes, leaf(edit.path, edit.Version, edit.Value, 32))
			*history = history.Join(edit.Version)
		}
	}
	return buildSorted(nodes, base)
}

func applyEdit[T any](
	n *node[T],
	edit preparedEdit[T],
	base uint8,
	history *before.Version,
) *node[T] {
	if edit.Insert {
		*history = history.Join(edit.Version)
		return put(n, leaf(edit.path, edit.Version, edit.Value, base), base)
	}
	resident := find(n, edit.path)
	if resident == nil || !resident.entry.version.LessEqual(edit.Stamp) {
		return n
	}
	*history = history.Join(edit.Stamp)
	return remove(n, edit.path)
}

type editFan[T any] struct {
	prior   []*node[T]
	result  []*node[T]
	next    int
	depth   uint8
	changed bool
}

func applyFan[T any](
	n *node[T],
	edits []preparedEdit[T],
	base, depth uint8,
	history *before.Version,
) *node[T] {
	prior := n.children
	if n.depth != depth {
		prior = []*node[T]{n}
	}
	fan := editFan[T]{
		prior:  prior,
		result: make([]*node[T], 0, min(256, len(prior)+len(edits))),
		depth:  depth,
	}
	for len(edits) > 0 {
		end := editGroupEnd(edits, depth)
		fan.apply(edits[:end], history)
		edits = edits[end:]
	}
	if !fan.changed {
		return n
	}
	fan.result = append(fan.result, prior[fan.next:]...)
	return branch(base, depth, fan.result)
}

func editGroupEnd[T any](edits []preparedEdit[T], depth uint8) int {
	end := 1
	for end < len(edits) && edits[end].path[depth] == edits[0].path[depth] {
		end++
	}
	return end
}

func (f *editFan[T]) apply(edits []preparedEdit[T], history *before.Version) {
	radix := edits[0].path[f.depth]
	index := childIndex(f.prior, f.depth, radix)
	f.result = append(f.result, f.prior[f.next:index]...)
	var prior *node[T]
	f.next = index
	if index < len(f.prior) && f.prior[index].path[f.depth] == radix {
		prior = f.prior[index]
		f.next++
	}
	updated := applyEdits(prior, edits, f.depth+1, history)
	f.changed = f.changed || updated != prior
	f.result = appendLive(f.result, updated)
}
