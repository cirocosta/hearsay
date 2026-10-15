package protocol

import "fmt"

// LabelError retains decoded label operands even when its epoch or index fails.
type LabelError struct {
	ExpectedEpoch uint8
	Epoch, Stream uint64
}

func (e *LabelError) Error() string {
	return fmt.Sprintf(
		"%v: expected epoch %d, received epoch %d stream %d",
		ErrStream,
		e.ExpectedEpoch,
		e.Epoch,
		e.Stream,
	)
}
func (e *LabelError) Unwrap() error { return ErrStream }
