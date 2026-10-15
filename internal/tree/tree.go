// Package tree implements an immutable, version-addressed Merkle radix trie.
// Payloads must remain immutable while reachable through any tree snapshot.
package tree

import (
	"crypto/sha3"
	"encoding/binary"
	"iter"
	"sort"

	"github.com/cirocosta/hearsay/before"
)

type (
	// Hash compares corresponding subtrees using a truncated SHA3-256 digest.
	Hash [24]byte
	// Path addresses a leaf using the full SHA3-256 digest of its version.
	Path [32]byte
)

// Address depends only on a version, never on the message payload.
func Address(v before.Version) Path { return sha3.Sum256(v.AppendBinary(nil)) }

type entry[T any] struct {
	version before.Version
	value   T
}

type node[T any] struct {
	path           Path
	base, depth    uint8
	children       []*node[T]
	entry          *entry[T]
	hash           Hash
	count          int
	versionBytes   int
	floor, ceiling before.Version
}

// Tree is a persistent snapshot. Copying it is O(1). Its zero value is empty.
type Tree[T any] struct {
	root    *node[T]
	ceiling before.Version
}

// WithLatest adds observed causal history, including history with no live leaf.
// Use this only after receiving a complete, coherent remote snapshot.
func (t Tree[T]) WithLatest(v before.Version) Tree[T] {
	t.ceiling = t.ceiling.Join(v)
	return t
}

// SameRoot detects a shared snapshot without reading its Merkle hash.
func (t Tree[T]) SameRoot(other Tree[T]) bool {
	return t.root == other.root && t.ceiling.Equal(other.ceiling)
}

// Len returns the number of live leaves.
func (t Tree[T]) Len() int {
	if t.root == nil {
		return 0
	}
	return t.root.count
}

// Latest returns observed causal history, including redactions.
func (t Tree[T]) Latest() before.Version { return t.ceiling }

// MaxVersionBytes returns the largest canonical encoding among all live node
// bounds. It excludes the root's independent causal history and is zero when empty.
func (t Tree[T]) MaxVersionBytes() int {
	if t.root == nil {
		return 0
	}
	return t.root.versionBytes
}

func versionBytes(v before.Version) int {
	var buf [64]byte
	return len(v.AppendBinary(buf[:0]))
}

// Earliest returns the meet of live versions, or false when empty.
func (t Tree[T]) Earliest() (before.Version, bool) {
	if t.root == nil {
		return before.Version{}, false
	}
	return t.root.floor, true
}

var emptyHash = digest([]byte{1, 0, 0, 0})

// Hash returns the live version set digest, excluding causal history.
func (t Tree[T]) Hash() Hash {
	if t.root == nil {
		return emptyHash
	}
	return t.root.hash
}
func digest(p []byte) Hash { s := sha3.Sum256(p); return Hash(s[:24]) }

func rehash[T any](n *node[T]) {
	if n.entry != nil {
		var buf [34]byte
		buf[1] = 32 - n.base
		copy(buf[2:], n.path[n.base:])
		n.hash = digest(buf[:34-int(n.base)])
		return
	}
	// a bounded stack buffer avoids a temporary heap allocation for branch hashes.
	var buf [4 + 32 + 256*25]byte
	buf[0], buf[1] = 1, n.depth-n.base
	i := 2 + copy(buf[2:], n.path[n.base:n.depth])
	binary.BigEndian.PutUint16(buf[i:i+2], uint16(len(n.children)))
	i += 2
	for _, c := range n.children {
		buf[i] = c.path[n.depth]
		i++
		i += copy(buf[i:], c.hash[:])
	}
	n.hash = digest(buf[:i])
}

func rebase[T any](n *node[T], base uint8) *node[T] {
	if n == nil || n.base == base {
		return n
	}
	c := *n
	c.base = base
	rehash(&c)
	return &c
}

func leaf[T any](path Path, v before.Version, value T, base uint8) *node[T] {
	n := &node[T]{
		path:    path,
		base:    base,
		depth:   32,
		entry:   &entry[T]{v, value},
		count:   1,
		floor:   v,
		ceiling: v,
	}
	n.versionBytes = versionBytes(v)
	rehash(n)
	return n
}

func branch[T any](base, depth uint8, children []*node[T]) *node[T] {
	if len(children) == 0 {
		return nil
	}
	if len(children) == 1 {
		return rebase(children[0], base)
	}
	n := &node[T]{
		path:     children[0].path,
		base:     base,
		depth:    depth,
		children: children,
		floor:    children[0].floor,
	}
	for i, c := range children {
		c = rebase(c, depth+1)
		children[i] = c
		n.count += c.count
		n.floor = n.floor.Meet(c.floor)
		n.ceiling = n.ceiling.Join(c.ceiling)
		n.versionBytes = max(n.versionBytes, c.versionBytes)
	}
	n.versionBytes = max(n.versionBytes, versionBytes(n.floor), versionBytes(n.ceiling))
	rehash(n)
	return n
}

func common(a, b Path, base, limit uint8) uint8 {
	for base < limit && a[base] == b[base] {
		base++
	}
	return base
}

func childIndex[T any](children []*node[T], depth uint8, radix byte) int {
	return sort.Search(len(children), func(i int) bool { return children[i].path[depth] >= radix })
}

func put[T any](n, item *node[T], base uint8) *node[T] {
	if n == nil {
		return rebase(item, base)
	}
	d := common(n.path, item.path, base, n.depth)
	if d < n.depth {
		children := []*node[T]{n, item}
		if n.path[d] > item.path[d] {
			children[0], children[1] = children[1], children[0]
		}
		return branch(base, d, children)
	}
	if n.entry != nil {
		return n
	}
	i := childIndex(n.children, n.depth, item.path[n.depth])
	children := make([]*node[T], len(n.children))
	copy(children, n.children)
	if i < len(children) && children[i].path[n.depth] == item.path[n.depth] {
		next := put(children[i], item, n.depth+1)
		if next == children[i] {
			return n
		}
		children[i] = next
	} else {
		children = append(children, nil)
		copy(children[i+1:], children[i:])
		children[i] = item
	}
	return branch(base, n.depth, children)
}

// Insert adds a version once. Reusing a version retains its original payload.
func (t Tree[T]) Insert(v before.Version, value T) Tree[T] {
	t.root = put(t.root, leaf(Address(v), v, value, 0), 0)
	t.ceiling = t.ceiling.Join(v)
	return t
}

// Get returns the live payload stamped with v.
func (t Tree[T]) Get(v before.Version) (T, bool) {
	n := find(t.root, Address(v))
	if n != nil && n.entry.version.Equal(v) {
		return n.entry.value, true
	}
	var zero T
	return zero, false
}

func find[T any](n *node[T], p Path) *node[T] {
	for n != nil {
		if common(n.path, p, n.base, n.depth) != n.depth {
			return nil
		}
		if n.entry != nil {
			return n
		}
		n = childAt(n, p[n.depth])
	}
	return nil
}

func childAt[T any](n *node[T], radix byte) *node[T] {
	i := childIndex(n.children, n.depth, radix)
	if i == len(n.children) || n.children[i].path[n.depth] != radix {
		return nil
	}
	return n.children[i]
}

func remove[T any](n *node[T], p Path) *node[T] {
	if n == nil || common(n.path, p, n.base, n.depth) != n.depth {
		return n
	}
	if n.entry != nil {
		return nil
	}
	i := childIndex(n.children, n.depth, p[n.depth])
	if i == len(n.children) || n.children[i].path[n.depth] != p[n.depth] {
		return n
	}
	c := remove(n.children[i], p)
	if c == n.children[i] {
		return n
	}
	children := append([]*node[T](nil), n.children...)
	if c == nil {
		children = append(children[:i], children[i+1:]...)
	} else {
		children[i] = c
	}
	return branch(n.base, n.depth, children)
}

// Forget removes a live version and retains the causal history of its deletion.
// A missing version has no effect. The deletion version must include the leaf.
func (t Tree[T]) Forget(v, deletion before.Version) Tree[T] {
	if !v.LessEqual(deletion) {
		return t
	}
	n := remove(t.root, Address(v))
	if n == t.root {
		return t
	}
	t.root = n
	t.ceiling = t.ceiling.Join(deletion)
	return t
}

// All walks live entries in address order and stops when yield returns false.
func (t Tree[T]) All() iter.Seq2[before.Version, T] {
	return func(yield func(before.Version, T) bool) { walk(t.root, yield) }
}

func walk[T any](n *node[T], yield func(before.Version, T) bool) bool {
	if n == nil {
		return true
	}
	if n.entry != nil {
		return yield(n.entry.version, n.entry.value)
	}
	for _, c := range n.children {
		if !walk(c, yield) {
			return false
		}
	}
	return true
}

// Range visits only versions matching accept. Bounds prune subtrees when the
// optional admit function can classify their floor and ceiling: -1 excludes,
// 0 descends, and 1 accepts the entire subtree.
func (t Tree[T]) Range(
	accept func(before.Version) bool,
	admit func(before.Version, before.Version) int,
) iter.Seq2[before.Version, T] {
	return func(yield func(before.Version, T) bool) {
		walkRange(t.root, accept, admit, yield)
	}
}

func walkRange[T any](
	n *node[T],
	accept func(before.Version) bool,
	admit func(before.Version, before.Version) int,
	yield func(before.Version, T) bool,
) bool {
	if n == nil {
		return true
	}
	if admit != nil {
		switch admit(n.floor, n.ceiling) {
		case -1:
			return true
		case 1:
			return walk(n, yield)
		}
	}
	if n.entry != nil {
		return !accept(n.entry.version) || yield(n.entry.version, n.entry.value)
	}
	return walkRangeChildren(n, accept, admit, yield)
}

func walkRangeChildren[T any](
	n *node[T],
	accept func(before.Version) bool,
	admit func(before.Version, before.Version) int,
	yield func(before.Version, T) bool,
) bool {
	for _, c := range n.children {
		if !walkRange(c, accept, admit, yield) {
			return false
		}
	}
	return true
}
