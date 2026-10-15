package tree

import (
	"bytes"
	"errors"
	"iter"

	"github.com/cirocosta/hearsay/before"
)

// ErrScope identifies an invalid prefix, child placement, or sorted leaf run.
var ErrScope = errors.New("tree: invalid scope")

// Scope is an immutable subtree viewed at an arbitrary logical prefix depth.
// It expands compressed spines without allocating or changing stored nodes.
// Its zero value is the empty root scope.
type Scope[T any] struct {
	prefix Path
	depth  uint8
	node   *node[T]
}

// Child is one occupied direct radix slot of a logical scope.
type Child[T any] struct {
	Radix byte
	Scope Scope[T]
}

// Leaf carries one immutable payload and its unique version for a sorted run.
type Leaf[T any] struct {
	Version before.Version
	Value   T
}

// Scope looks up a logical prefix, including positions inside compressed nodes.
// An absent prefix returns an empty scope at the requested position.
func (t Tree[T]) Scope(prefix []byte) (Scope[T], error) {
	scope, err := emptyScope[T](prefix)
	if err != nil {
		return scope, err
	}
	scope.node = scopeNode(t.root, scope.prefix, scope.depth)
	return scope, nil
}

func emptyScope[T any](prefix []byte) (Scope[T], error) {
	if len(prefix) > 32 {
		return Scope[T]{}, ErrScope
	}
	scope := Scope[T]{depth: uint8(len(prefix))}
	copy(scope.prefix[:], prefix)
	return scope, nil
}

func scopeNode[T any](n *node[T], prefix Path, depth uint8) *node[T] {
	for n != nil {
		if common(n.path, prefix, n.base, min(n.depth, depth)) != min(n.depth, depth) {
			return nil
		}
		if depth <= n.depth {
			return n
		}
		n = childAt(n, prefix[n.depth])
	}
	return nil
}

// Depth returns the number of prefix bytes consumed by this logical view.
func (s Scope[T]) Depth() int { return int(s.depth) }

// Prefix returns a detached copy of the scope's preceding address bytes.
func (s Scope[T]) Prefix() []byte { return append([]byte(nil), s.prefix[:s.depth]...) }

// Len returns the number of live leaves under this prefix.
func (s Scope[T]) Len() int {
	if s.node == nil {
		return 0
	}
	return s.node.count
}

// Hash returns the digest rebased to this exact logical trie position.
func (s Scope[T]) Hash() Hash {
	if s.node == nil {
		return emptyHash
	}
	if s.node.base == s.depth {
		return s.node.hash
	}
	view := *s.node
	view.base = s.depth
	rehash(&view)
	return view.hash
}

// Floor returns the meet of live versions, or the empty version when absent.
func (s Scope[T]) Floor() before.Version {
	if s.node == nil {
		return before.Version{}
	}
	return s.node.floor
}

// Ceiling returns the join of live versions, excluding independent tree history.
func (s Scope[T]) Ceiling() before.Version {
	if s.node == nil {
		return before.Version{}
	}
	return s.node.ceiling
}

// MaxVersionBytes returns the largest stored node-bound encoding in this scope.
func (s Scope[T]) MaxVersionBytes() int {
	if s.node == nil {
		return 0
	}
	return s.node.versionBytes
}

// Children lists occupied direct slots in ascending radix order. A compressed
// spine yields one virtual child whose hash drops exactly one prefix byte.
func (s Scope[T]) Children() []Child[T] {
	if s.node == nil || s.depth == 32 {
		return nil
	}
	if s.depth < s.node.depth {
		return []Child[T]{s.child(s.node.path[s.depth], s.node)}
	}
	children := make([]Child[T], len(s.node.children))
	for i, n := range s.node.children {
		children[i] = s.child(n.path[s.depth], n)
	}
	return children
}

func (s Scope[T]) child(radix byte, n *node[T]) Child[T] {
	prefix := s.prefix
	prefix[s.depth] = radix
	return Child[T]{Radix: radix, Scope: Scope[T]{prefix: prefix, depth: s.depth + 1, node: n}}
}

// Leaf returns the payload only at a populated full-width address scope.
func (s Scope[T]) Leaf() (before.Version, T, bool) {
	if s.depth == 32 && s.node != nil && s.node.entry != nil {
		return s.node.entry.version, s.node.entry.value, true
	}
	var zero T
	return before.Version{}, zero, false
}

// All walks all leaves under this scope in ascending address order.
func (s Scope[T]) All() iter.Seq2[before.Version, T] {
	return func(yield func(before.Version, T) bool) { walk(s.node, yield) }
}

// WithChildren reconstructs this logical position from ordered direct slots.
// It canonicalizes compression and preserves this scope when children agree.
func (s Scope[T]) WithChildren(children []Child[T]) (Scope[T], error) {
	if s.depth == 32 {
		return s, ErrScope
	}
	if err := s.checkChildren(children); err != nil {
		return s, err
	}
	nodes := make([]*node[T], len(children))
	for i, child := range children {
		nodes[i] = rebase(child.Scope.node, s.depth+1)
	}
	result := s
	result.node = branch(s.depth, s.depth, nodes)
	if s.node != nil && result.node != nil && s.Hash() == result.Hash() {
		return s, nil
	}
	return result, nil
}

func (s Scope[T]) checkChildren(children []Child[T]) error {
	for i, child := range children {
		if i > 0 && children[i-1].Radix >= child.Radix {
			return ErrScope
		}
		if !s.containsChild(child) {
			return ErrScope
		}
	}
	return nil
}

func (s Scope[T]) containsChild(child Child[T]) bool {
	return child.Scope.node != nil && child.Scope.depth == s.depth+1 &&
		child.Scope.prefix[s.depth] == child.Radix &&
		bytes.Equal(s.prefix[:s.depth], child.Scope.prefix[:s.depth])
}

// LeafScope constructs the full-address logical view of a single new leaf.
func LeafScope[T any](v before.Version, value T) Scope[T] {
	path := Address(v)
	return Scope[T]{prefix: path, depth: 32, node: leaf(path, v, value, 32)}
}

// BuildScope constructs a canonical subtree from a strictly address-ordered run.
// Every leaf must lie under prefix. Empty runs produce an absent scope.
func BuildScope[T any](prefix []byte, leaves []Leaf[T]) (Scope[T], error) {
	scope, err := emptyScope[T](prefix)
	if err != nil {
		return scope, err
	}
	nodes, err := scope.sortedLeaves(leaves)
	if err != nil {
		return scope, err
	}
	scope.node = buildSorted(nodes, scope.depth)
	return scope, nil
}

func (s Scope[T]) sortedLeaves(leaves []Leaf[T]) ([]*node[T], error) {
	nodes := make([]*node[T], len(leaves))
	for i, item := range leaves {
		path := Address(item.Version)
		if !bytes.Equal(path[:s.depth], s.prefix[:s.depth]) {
			return nil, ErrScope
		}
		if i > 0 && bytes.Compare(nodes[i-1].path[:], path[:]) >= 0 {
			return nil, ErrScope
		}
		nodes[i] = leaf(path, item.Version, item.Value, 32)
	}
	return nodes, nil
}

func buildSorted[T any](nodes []*node[T], base uint8) *node[T] {
	switch len(nodes) {
	case 0:
		return nil
	case 1:
		return rebase(nodes[0], base)
	}
	depth := common(nodes[0].path, nodes[len(nodes)-1].path, base, 32)
	children := make([]*node[T], 0, min(len(nodes), 256))
	for start := 0; start < len(nodes); {
		end := start + 1
		for end < len(nodes) && nodes[start].path[depth] == nodes[end].path[depth] {
			end++
		}
		children = append(children, buildSorted(nodes[start:end], depth+1))
		start = end
	}
	return branch(base, depth, children)
}

// Tree publishes a root scope with causal history covering all its live leaves.
// Nonroot scopes cannot be converted into whole replica trees.
func (s Scope[T]) Tree(latest before.Version) (Tree[T], error) {
	if s.depth != 0 {
		return Tree[T]{}, ErrScope
	}
	return Tree[T]{root: rebase(s.node, 0), ceiling: latest.Join(s.Ceiling())}, nil
}

// ReplaceScope splices a complete subtree at its logical prefix. It preserves
// observed history and adds any previously unknown live version history.
func (t Tree[T]) ReplaceScope(scope Scope[T]) (Tree[T], error) {
	if scope.depth > 32 {
		return t, ErrScope
	}
	t.root = replaceScope(t.root, scope.node, scope.prefix, scope.depth, 0)
	t.ceiling = t.ceiling.Join(scope.Ceiling())
	return t, nil
}

func replaceScope[T any](n, replacement *node[T], prefix Path, depth, base uint8) *node[T] {
	if n == nil {
		return rebase(replacement, base)
	}
	limit := min(n.depth, depth)
	diverge := common(n.path, prefix, base, limit)
	if diverge < limit {
		return replaceDisjoint(n, replacement, base, diverge)
	}
	if depth <= n.depth {
		return rebase(replacement, base)
	}
	return replaceScopeChild(n, replacement, prefix, depth)
}

func replaceDisjoint[T any](n, replacement *node[T], base, depth uint8) *node[T] {
	if replacement == nil {
		return n
	}
	children := []*node[T]{n, replacement}
	if n.path[depth] > replacement.path[depth] {
		children[0], children[1] = replacement, n
	}
	return branch(base, depth, children)
}

func replaceScopeChild[T any](n, replacement *node[T], prefix Path, depth uint8) *node[T] {
	i := childIndex(n.children, n.depth, prefix[n.depth])
	if i < len(n.children) && n.children[i].path[n.depth] == prefix[n.depth] {
		return replacePresentChild(n, replacement, prefix, depth, i)
	}
	if replacement == nil {
		return n
	}
	children := make([]*node[T], len(n.children)+1)
	copy(children, n.children[:i])
	children[i] = replacement
	copy(children[i+1:], n.children[i:])
	return branch(n.base, n.depth, children)
}

func replacePresentChild[T any](
	n, replacement *node[T],
	prefix Path,
	depth uint8,
	index int,
) *node[T] {
	child := replaceScope(n.children[index], replacement, prefix, depth, n.depth+1)
	if child == n.children[index] {
		return n
	}
	children := append([]*node[T](nil), n.children...)
	if child == nil {
		children = append(children[:index], children[index+1:]...)
	} else {
		children[index] = child
	}
	return branch(n.base, n.depth, children)
}
