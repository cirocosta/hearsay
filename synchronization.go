package hearsay

import "github.com/cirocosta/hearsay/internal/protocol"

// DefaultSyncMemoryBudget is the reconciliation pipeline's default byte target.
const DefaultSyncMemoryBudget uint64 = 512 << 20

// SynchronizationSettings are local inputs to per-session negotiation. Zero
// TargetMessageSize sends one leaf per frame. Zero SyncMemoryBudget uses the
// minimum pipeline. PayloadDepthLimit counts typed decoder recursion and must
// match at both ends. Zero admits values that require no recursive descent.
type SynchronizationSettings struct {
	// SyncMemoryBudget targets pending comparison metadata across 17 queues.
	// Queues charge allocated slots and detached child fans. Each queue permits
	// one item for progress even if it exceeds its share. Resize can temporarily
	// retain old and new slot arrays. Opening snapshots, reply/run buffers,
	// results, observers, and transport storage have separate ownership; this
	// setting does not cap the session's total resident memory.
	SyncMemoryBudget uint64

	TargetMessageSize uint64
	PayloadDepthLimit uint64
}

func defaultSettings(depth uint64) SynchronizationSettings {
	if depth == 0 {
		depth = DefaultPayloadDepthLimit
	}
	return SynchronizationSettings{
		DefaultSyncMemoryBudget,
		protocol.DefaultTargetMessageSize,
		depth,
	}
}

// SynchronizationSettings returns this replica's session parameters.
func (p *Peer[T]) SynchronizationSettings() SynchronizationSettings {
	p.state.mu.Lock()
	defer p.state.mu.Unlock()
	return p.state.settings
}

// SynchronizationSettings returns this replica's session parameters.
func (r *Rumors[T]) SynchronizationSettings() SynchronizationSettings {
	r.state.mu.Lock()
	defer r.state.mu.Unlock()
	return r.state.settings
}

// WithSynchronizationSettings configures an exclusively owned peer. Once
// IntoRumors consumes it, configuration through old peer copies is rejected.
func (p *Peer[T]) WithSynchronizationSettings(settings SynchronizationSettings) (*Peer[T], error) {
	if p == nil || p.state == nil {
		return nil, ErrClosed
	}
	s := p.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if p.token == nil || !p.token.alive {
		return nil, ErrClosed
	}
	settings.TargetMessageSize = min(settings.TargetMessageSize, protocol.MaxTargetMessageSize)
	s.settings = settings
	return p, nil
}

// WithSynchronizationSettings returns a configured copy for this and later
// joined-peer sessions. The original bootstrap builder remains unchanged.
func (b *BootstrapConfig[T]) WithSynchronizationSettings(
	settings SynchronizationSettings,
) (*BootstrapConfig[T], error) {
	if b == nil {
		return nil, ErrCodec
	}
	settings.TargetMessageSize = min(settings.TargetMessageSize, protocol.MaxTargetMessageSize)
	codec := b.codec
	codec.PayloadDepthLimit = settings.PayloadDepthLimit
	configured := *b
	configured.codec = codec
	configured.settings = settings
	return &configured, nil
}

// SynchronizationSettings returns this bootstrap attempt's session parameters.
func (b *BootstrapConfig[T]) SynchronizationSettings() SynchronizationSettings { return b.settings }
