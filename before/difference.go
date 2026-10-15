package before

func subtractIdentity(a, b *identity) *identity {
	if a == nil || b == owned {
		return nil
	}
	if b == nil {
		return a
	}
	al, ar := a.left, a.right
	if a == owned {
		al, ar = owned, owned
	}
	return identityBranch(subtractIdentity(al, b.left), subtractIdentity(ar, b.right))
}

// Without consumes p and returns the identity regions it owned outside other.
// It returns nil when other covers p. The receiver is retired in all cases;
// other is read only and remains live, unless it is the receiver itself.
func (p *Party) Without(other *Party) *Party {
	a, b := p.alive(), other.alive()
	root := subtractIdentity(a, b)
	p.root = nil
	if root == nil {
		return nil
	}
	return &Party{root: root}
}
