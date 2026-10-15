package reconcile

import (
	"bytes"
	"fmt"

	"github.com/cirocosta/hearsay/internal/protocol"
	"github.com/cirocosta/hearsay/internal/sessionerr"
	"github.com/cirocosta/hearsay/internal/tree"
)

type replyState struct {
	query       pending
	prefix      []byte
	cursor      int
	last        int
	active      bool
	supplyRadix byte
	previous    tree.Path
	supplied    bool
	reacted     bool
	early       bool
}

func newReplyState(q pending) *replyState {
	prefix := q.scope.Prefix()
	if q.terminal {
		prefix = prefix[:31]
	}
	return &replyState{query: q, prefix: prefix, last: -1}
}

func (e *engine) resolve(q pending, depth int, stream uint8) error {
	state := newReplyState(q)
	for {
		frame, err := e.streams.receive(stream)
		if err != nil {
			return e.streams.incomingError(int(stream), err)
		}
		ended, err := e.react(state, frame, depth)
		if err != nil {
			return e.streams.incomingError(int(stream), err)
		}
		if ended {
			if state.cursor != len(q.fan) {
				return e.streams.incomingError(
					int(stream),
					fmt.Errorf("%w: unanswered held child", ErrReply),
				)
			}
			return nil
		}
	}
}

func (e *engine) react(state *replyState, frame protocol.Frame, depth int) (bool, error) {
	switch frame.Signal {
	case protocol.ReplyEnd:
		if state.reacted {
			return false, fmt.Errorf("%w: bare end after reaction", ErrReply)
		}
		return true, nil
	case protocol.StreamEnd:
		return false, fmt.Errorf("%w: stream ended inside reply", ErrReply)
	case protocol.SupplyContinue, protocol.SupplyEnd:
		if err := e.absorbRun(state, frame.Run); err != nil {
			return false, err
		}
	case protocol.MatchContinue, protocol.MatchEnd:
		if _, err := state.consumeHeld(); err != nil {
			return false, err
		}
	case protocol.QueryContinue,
		protocol.QueryEnd,
		protocol.QueryEmptyContinue,
		protocol.QueryEmptyEnd:
		if err := e.reactQuery(state, frame, depth); err != nil {
			return false, err
		}
	default:
		return false, ErrReply
	}
	state.reacted = true
	return frame.Signal%2 == 1, nil
}

func (e *engine) reactQuery(state *replyState, frame protocol.Frame, depth int) error {
	child, err := state.consumeHeld()
	if err != nil {
		return err
	}
	return e.answerQuestion(child.Scope, frame.Listing, depth)
}

func (state *replyState) consumeHeld() (tree.Child[[]byte], error) {
	if state.query.terminal || state.cursor >= len(state.query.fan) {
		return tree.Child[[]byte]{}, fmt.Errorf("%w: unpositioned reaction", ErrReply)
	}
	child := state.query.fan[state.cursor]
	state.cursor++
	state.last = int(child.Radix)
	state.active = false
	return child, nil
}

func (e *engine) answerQuestion(
	scope tree.Scope[[]byte],
	theirs []protocol.Child,
	depth int,
) error {
	if len(theirs) == 0 && e.earlySurvivor(scope) {
		stream := outgoingStream(depth, e.role)
		return e.emit(nil, depth, stream)
	}
	return e.answer(scope, theirs, depth)
}

func (state *replyState) validatePath(path tree.Path) error {
	if len(state.prefix) >= 32 || !bytes.Equal(path[:len(state.prefix)], state.prefix) {
		return fmt.Errorf("%w: leaf outside reply prefix", ErrSupply)
	}
	if state.supplied && bytes.Compare(path[:], state.previous[:]) <= 0 {
		return fmt.Errorf("%w: leaf paths are not ascending", ErrSupply)
	}
	if state.query.terminal && !bytes.Equal(path[:], state.query.scope.Prefix()) {
		return fmt.Errorf("%w: wrong terminal leaf", ErrSupply)
	}
	return nil
}

func (state *replyState) observe(path tree.Path) error {
	if err := state.validatePath(path); err != nil {
		return err
	}
	radix := path[len(state.prefix)]
	if !state.active || state.supplyRadix != radix {
		if err := state.startSupply(radix); err != nil {
			return err
		}
	}
	state.previous = path
	state.supplied = true
	return nil
}

func (state *replyState) startSupply(radix byte) error {
	if int(radix) <= state.last {
		return fmt.Errorf("%w: supply radix is not ascending", ErrSupply)
	}
	if state.cursor < len(state.query.fan) && radix >= state.query.fan[state.cursor].Radix {
		return fmt.Errorf("%w: supply overlaps unanswered held child", ErrSupply)
	}
	state.active = true
	state.supplyRadix = radix
	state.last = int(radix)
	return nil
}

func (e *engine) absorbRun(state *replyState, run protocol.LeafRun) error {
	iterator := run.Records(e.recordLimits())
	for {
		record, ok, err := iterator.Next()
		if err != nil {
			return sessionerr.Protocol(err)
		}
		if !ok {
			return nil
		}
		if err := e.absorbRecord(state, record); err != nil {
			return err
		}
	}
}

func (e *engine) absorbRecord(state *replyState, record protocol.Record) error {
	path := tree.Address(record.Version)
	if state.early {
		if err := e.validateEarlyPath(path); err != nil {
			return err
		}
	}
	if err := state.observe(path); err != nil {
		return err
	}
	if !record.Version.LessEqual(e.remote.Version) {
		return ErrContainment
	}
	if e.received.Add(1) > e.remote.SetLen {
		return ErrSetLength
	}
	if err := e.admitPayload(record.Payload); err != nil {
		return err
	}
	if err := e.publish(tree.LeafScope(record.Version, bytes.Clone(record.Payload))); err != nil {
		return err
	}
	e.gained.Add(1)
	return nil
}

func (e *engine) admitPayload(payload []byte) error {
	if e.options.Admit != nil {
		return sessionerr.Protocol(e.options.Admit(payload))
	}
	return nil
}

func (e *engine) validateEarlyPath(path tree.Path) error {
	for _, child := range e.local.Listing {
		if child.Radix == path[0] {
			return fmt.Errorf("%w: early supply overlaps root", ErrSupply)
		}
	}
	for _, child := range e.remote.Listing {
		if child.Radix == path[0] {
			return nil
		}
	}
	return fmt.Errorf("%w: unannounced early supply radix", ErrSupply)
}

func (e *engine) receiveEarly() error {
	if len(exclusive(e.remote.Listing, e.local.Listing)) == 0 {
		return nil
	}
	root, _ := e.original.Scope(nil)
	state := newReplyState(pending{scope: root})
	state.early = true
	// early supplies self-key and never consume a local fan. Later empty
	// pairing replies leave these already-published leaves intact.
	for {
		frame, err := e.streams.receive(0)
		if err != nil {
			return err
		}
		ended, err := e.earlyFrame(state, frame)
		if err != nil {
			return e.streams.incomingError(0, err)
		}
		if ended {
			return e.streams.finishReceive(0)
		}
	}
}

func (e *engine) earlyFrame(state *replyState, frame protocol.Frame) (bool, error) {
	switch frame.Signal {
	case protocol.ReplyEnd:
		if state.reacted {
			return false, ErrReply
		}
		return true, nil
	case protocol.SupplyContinue, protocol.SupplyEnd:
		if err := e.absorbRun(state, frame.Run); err != nil {
			return false, err
		}
		state.reacted = true
		return frame.Signal == protocol.SupplyEnd, nil
	default:
		return false, ErrReply
	}
}
