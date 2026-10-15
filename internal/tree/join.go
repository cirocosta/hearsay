package tree

import "github.com/cirocosta/hearsay/before"

// Join merges replicas and honors causal redactions without per-leaf tombstones.
// An absent leaf is deleted if its version is covered by that side's history.
func (t Tree[T]) Join(other Tree[T]) Tree[T] {
	return Tree[T]{
		root:    merge(t.root, other.root, t.ceiling, other.ceiling, 0),
		ceiling: t.ceiling.Join(other.ceiling),
	}
}

func filter[T any](n *node[T], seen before.Version, base uint8) *node[T] {
	if n == nil {
		return nil
	}
	if n.ceiling.LessEqual(seen) {
		return nil
	}
	if !n.floor.LessEqual(seen) {
		return rebase(n, base)
	}
	if n.entry != nil {
		return rebase(n, base)
	}
	return filterChildren(n, seen, base)
}

func filterChildren[T any](n *node[T], seen before.Version, base uint8) *node[T] {
	children := make([]*node[T], 0, len(n.children))
	changed := false
	for _, c := range n.children {
		next := filter(c, seen, n.depth+1)
		if next != c {
			changed = true
		}
		if next != nil {
			children = append(children, next)
		}
	}
	if !changed {
		return rebase(n, base)
	}
	return branch(base, n.depth, children)
}

func merge[T any](a, b *node[T], av, bv before.Version, base uint8) *node[T] {
	if a == nil {
		return filter(b, av, base)
	}
	if b == nil {
		return filter(a, bv, base)
	}
	a = rebase(a, base)
	b = rebase(b, base)
	if a == b || a.hash == b.hash {
		return a
	}
	limit := min(a.depth, b.depth)
	d := common(a.path, b.path, base, limit)
	if d < limit {
		return mergeDisjoint(a, b, av, bv, base, d)
	}
	if a.depth > b.depth {
		return merge(b, a, bv, av, base)
	}
	if a.depth < b.depth {
		return branch(base, a.depth, mergeFans(a.children, []*node[T]{b}, av, bv, a.depth))
	}
	// equal leaf addresses imply the same version under the address collision
	// assumption. Their hashes would already have matched above.
	if a.entry != nil {
		return a
	}
	return branch(base, a.depth, mergeFans(a.children, b.children, av, bv, a.depth))
}

func mergeDisjoint[T any](a, b *node[T], av, bv before.Version, base, depth uint8) *node[T] {
	a = filter(a, bv, depth+1)
	b = filter(b, av, depth+1)
	if a == nil {
		return rebase(b, base)
	}
	if b == nil {
		return rebase(a, base)
	}
	children := []*node[T]{a, b}
	if a.path[depth] > b.path[depth] {
		children[0], children[1] = b, a
	}
	return branch(base, depth, children)
}

func appendLive[T any](children []*node[T], n *node[T]) []*node[T] {
	if n != nil {
		return append(children, n)
	}
	return children
}

func appendFiltered[T any](
	children, remaining []*node[T],
	seen before.Version,
	base uint8,
) []*node[T] {
	for _, n := range remaining {
		children = appendLive(children, filter(n, seen, base))
	}
	return children
}

// mergeFans always owns its output slice; branch may rebase entries in place.
func mergeFans[T any](a, b []*node[T], av, bv before.Version, depth uint8) []*node[T] {
	children := make([]*node[T], 0, max(len(a), len(b)))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		left, right := a[i], b[j]
		var next *node[T]
		switch {
		case left.path[depth] < right.path[depth]:
			next = filter(left, bv, depth+1)
			i++
		case left.path[depth] > right.path[depth]:
			next = filter(right, av, depth+1)
			j++
		default:
			next = merge(left, right, av, bv, depth+1)
			i++
			j++
		}
		children = appendLive(children, next)
	}
	children = appendFiltered(children, a[i:], bv, depth+1)
	return appendFiltered(children, b[j:], av, depth+1)
}
