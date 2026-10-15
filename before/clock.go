package before

// Clock pairs exclusive identity with an immutable causal timestamp. Do not copy
// a Clock. Fork creates an independent participant, while Version is a snapshot.
type Clock struct {
	party   Party
	version Version
}

// Seed creates the first clock in a causal universe.
func Seed() *Clock { return &Clock{party: Party{root: owned}} }

// FromParts transfers identity p into a clock with history v.
func FromParts(p *Party, v Version) *Clock {
	c := &Clock{party: Party{root: p.alive()}, version: v}
	p.root = nil
	return c
}

// Version returns an immutable snapshot of the current history.
func (c *Clock) Version() Version { c.party.alive(); return c.version }

// Party returns the clock's identity for read-only operations. Mutating the
// returned party changes this clock's identity and requires the same ownership.
func (c *Clock) Party() *Party { c.party.alive(); return &c.party }

// Fork transfers half the identity to a new clock sharing the current history.
func (c *Clock) Fork() *Clock { p := c.party.Fork(); return FromParts(p, c.version) }

// Tick records one local event and returns the new history.
func (c *Clock) Tick() Version { c.version = c.version.Tick(&c.party); return c.version }

// Ticks records n local events and returns the new history.
func (c *Clock) Ticks(
	n uint64,
) Version {
	c.version = c.version.Ticks(&c.party, n)
	return c.version
}

// Absorb merges a received history without recording a local event.
func (c *Clock) Absorb(v Version) Version {
	c.party.alive()
	c.version = c.version.Join(v)
	return c.version
}

// AbsorbAll merges received histories without recording a local event.
func (c *Clock) AbsorbAll(v ...Version) Version {
	c.party.alive()
	c.version = c.version.JoinAll(v...)
	return c.version
}

// Send records a send event and returns its history.
func (c *Clock) Send() Version { return c.Tick() }

// Recv merges a received history and then records a receive event.
func (c *Clock) Recv(v Version) Version { c.Absorb(v); return c.Tick() }

// RecvAll merges received histories and records one receive event.
func (c *Clock) RecvAll(v ...Version) Version { c.AbsorbAll(v...); return c.Tick() }

// Join transfers the donor identity and history into this clock.
func (c *Clock) Join(d *Clock) error {
	if err := c.party.Join(&d.party); err != nil {
		return err
	}
	c.version = c.version.Join(d.version)
	d.version = Version{}
	return nil
}

// Sync gives two disjoint clocks the union of their histories.
func (c *Clock) Sync(d *Clock) error {
	if !c.party.IsDisjoint(&d.party) {
		return ErrOverlap
	}
	v := c.version.Join(d.version)
	c.version = v
	d.version = v
	return nil
}

// OwnVersion returns the history within the owned identity region.
func (c *Clock) OwnVersion() Version { return c.version.Project(&c.party) }

// IntoParts retires the clock and transfers its identity and history.
func (c *Clock) IntoParts() (*Party, Version) {
	p := &Party{root: c.party.alive()}
	c.party.root = nil
	v := c.version
	c.version = Version{}
	return p, v
}
