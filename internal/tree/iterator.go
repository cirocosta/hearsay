package tree

import "github.com/cirocosta/hearsay/before"

type frame[T any] struct {
	node *node[T]
	next int
	all  bool
}

// Iterator is a lazy snapshot walk in address order. It uses a bounded stack
// instead of a goroutine or an eagerly collected backlog. Do not share an
// Iterator between goroutines without synchronization.
type Iterator[T any] struct {
	frames   [33]frame[T]
	depth    int
	since    before.Version
	filtered bool
}

// Iterator creates an independent walk over this immutable snapshot.
func (t Tree[T]) Iterator() *Iterator[T] {
	iterator := &Iterator[T]{}
	iterator.push(t.root, true)
	return iterator
}

// Since visits versions not causally covered by since. Cached bounds prune
// whole subtrees and bypass individual comparisons for wholly admitted subtrees.
func (t Tree[T]) Since(since before.Version) *Iterator[T] {
	iterator := &Iterator[T]{since: since, filtered: true}
	iterator.push(t.root, false)
	return iterator
}

// Next yields the next live entry. Once exhausted, it always returns false.
func (it *Iterator[T]) Next() (before.Version, T, bool) {
	for it.depth > 0 {
		top := &it.frames[it.depth-1]
		if top.node.entry != nil {
			entry := top.node.entry
			it.pop()
			return entry.version, entry.value, true
		}
		if top.next == len(top.node.children) {
			it.pop()
			continue
		}
		child := top.node.children[top.next]
		all := top.all
		top.next++
		it.push(child, all)
	}
	var zero T
	return before.Version{}, zero, false
}

func (it *Iterator[T]) push(n *node[T], all bool) {
	if n == nil {
		return
	}
	if it.filtered && !all {
		if n.ceiling.LessEqual(it.since) {
			return
		}
		all = !n.floor.LessEqual(it.since)
	}
	it.frames[it.depth] = frame[T]{node: n, all: all}
	it.depth++
}

func (it *Iterator[T]) pop() {
	it.depth--
	it.frames[it.depth] = frame[T]{}
}
