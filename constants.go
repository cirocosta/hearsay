package hearsay

import (
	record "github.com/cirocosta/hearsay/internal/bookmark"
	"github.com/cirocosta/hearsay/internal/protocol"
)

// CBOR tags identify opaque causal atoms in V2 captures and bookmarks.
// They are provisional protocol vocabulary, not application payload tags.
const (
	PartyTag   = protocol.PartyTag
	VersionTag = protocol.VersionTag
	ClockTag   = protocol.ClockTag
)

// BookmarkFormatVersion identifies the persisted recovery record format.
const BookmarkFormatVersion = record.FormatVersion

// DefaultTargetMessageSize is the default whole-frame supply target in bytes.
const DefaultTargetMessageSize = protocol.DefaultTargetMessageSize

// MaxRunBudgetBytes is the largest target that fits V2's run-length field.
// Synchronization settings saturate larger targets at this value.
const MaxRunBudgetBytes = protocol.MaxTargetMessageSize
