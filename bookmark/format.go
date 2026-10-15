package bookmark

import (
	"errors"
	"fmt"
)

// FormatVersion is the encoded restart record version written by this build.
const FormatVersion uint64 = 6

// Format error categories are stable targets for errors.Is.
var (
	// ErrTruncated reports a frame that ends before all fields are present.
	ErrTruncated = errors.New("bookmark: truncated frame")
	// ErrNotBookmark reports a malformed envelope or a wrong required tag.
	ErrNotBookmark = errors.New("bookmark: invalid frame")
	// ErrVersion reports an unsupported bookmark format version.
	ErrVersion = errors.New("bookmark: unsupported format version")
	// ErrChecksum reports an integrity hash mismatch.
	ErrChecksum = errors.New("bookmark: checksum mismatch")
	// ErrRecord reports invalid record structure or canonical atom bytes.
	ErrRecord = errors.New("bookmark: invalid recovery record")
)

// FormatError reports why stored bytes cannot supply recovery rights. A storage
// backend's load failure is returned separately as its original error.
// FoundVersion applies to ErrVersion and Length applies to ErrTruncated.
// Err preserves the underlying structural or atom decoding failure, if any.
type FormatError struct {
	Kind         error
	FoundVersion uint64
	Length       int
	Err          error
}

func (e *FormatError) Error() string {
	switch e.Kind {
	case ErrTruncated:
		return fmt.Sprintf(
			"bookmark truncated: the %d bytes present end inside the frame",
			e.Length,
		)
	case ErrVersion:
		return fmt.Sprintf(
			"unsupported bookmark format version %d (this build writes %d)",
			e.FoundVersion,
			FormatVersion,
		)
	case ErrChecksum:
		return "bookmark integrity hash mismatch: stored record is corrupt"
	case ErrRecord:
		return fmt.Sprintf("the bookmark payload is not a valid recovery record: %v", e.Err)
	default:
		return fmt.Sprintf("hearsay: invalid bookmark envelope: %v", e.Err)
	}
}

// Unwrap returns the underlying structural or atom error.
func (e *FormatError) Unwrap() error { return e.Err }

// Is matches one of the format error categories while Unwrap preserves details.
func (e *FormatError) Is(target error) bool { return errors.Is(e.Kind, target) }
