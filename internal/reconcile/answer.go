package reconcile

import (
	"errors"
	"fmt"

	"github.com/cirocosta/hearsay/internal/protocol"
	"github.com/cirocosta/hearsay/internal/tree"
)

type reaction struct {
	signal   protocol.Signal
	listing  []protocol.Child
	supply   tree.Scope[[]byte]
	question *pending
	flushed  chan struct{}
}

func (e *engine) answer(scope tree.Scope[[]byte], theirs []protocol.Child, depth int) error {
	if depth == 32 {
		return e.answerLeaf(scope, theirs)
	}
	actions, err := e.compare(scope, theirs, depth)
	if err != nil {
		return err
	}
	return e.emit(actions, depth, outgoingStream(depth, e.role))
}

func (e *engine) answerLeaf(scope tree.Scope[[]byte], theirs []protocol.Child) error {
	if len(theirs) != 0 {
		return fmt.Errorf("%w: nonempty terminal question", ErrReply)
	}
	survivor, err := e.filter(scope)
	if err != nil {
		return err
	}
	if err := e.publish(survivor); err != nil {
		return err
	}
	var actions []reaction
	if survivor.Len() > 0 {
		actions = append(actions, reaction{signal: protocol.SupplyContinue, supply: survivor})
	}
	return e.emit(actions, 32, 16)
}

type fanPair struct {
	ours   *tree.Child[[]byte]
	theirs *protocol.Child
}
type fanMerge struct {
	ours   []tree.Child[[]byte]
	theirs []protocol.Child
	i, j   int
}

func (m *fanMerge) next() (fanPair, bool) {
	if m.i == len(m.ours) && m.j == len(m.theirs) {
		return fanPair{}, false
	}
	var pair fanPair
	switch {
	case m.i == len(m.ours):
		pair.theirs = &m.theirs[m.j]
		m.j++
	case m.j == len(m.theirs):
		pair.ours = &m.ours[m.i]
		m.i++
	case m.ours[m.i].Radix < m.theirs[m.j].Radix:
		pair.ours = &m.ours[m.i]
		m.i++
	case m.ours[m.i].Radix > m.theirs[m.j].Radix:
		pair.theirs = &m.theirs[m.j]
		m.j++
	default:
		pair.ours = &m.ours[m.i]
		pair.theirs = &m.theirs[m.j]
		m.i++
		m.j++
	}
	return pair, true
}

func (e *engine) compare(
	scope tree.Scope[[]byte],
	theirs []protocol.Child,
	depth int,
) ([]reaction, error) {
	ours := scope.Children()
	merge := fanMerge{ours: ours, theirs: theirs}
	actions := make([]reaction, 0, len(ours)+len(theirs))
	differed := false
	for pair, ok := merge.next(); ok; pair, ok = merge.next() {
		action, different, err := e.comparePair(scope, pair, depth)
		if err != nil {
			return nil, err
		}
		differed = differed || different
		if action != nil {
			actions = append(actions, *action)
		}
	}
	if len(ours) > 0 && len(theirs) > 0 && differed {
		e.disputed.Add(1)
	}
	return actions, nil
}

func (e *engine) comparePair(
	scope tree.Scope[[]byte],
	pair fanPair,
	depth int,
) (*reaction, bool, error) {
	if pair.ours == nil {
		prefix := append(scope.Prefix(), pair.theirs.Radix)
		q := pending{scope: emptyScope(prefix), terminal: depth == 31}
		return &reaction{signal: protocol.QueryEmptyContinue, question: &q}, true, nil
	}
	if pair.theirs == nil {
		return e.compareExclusive(pair.ours.Scope)
	}
	if depth == 31 || [24]byte(pair.ours.Scope.Hash()) == pair.theirs.Hash {
		return &reaction{signal: protocol.MatchContinue}, false, nil
	}
	fan := pair.ours.Scope.Children()
	q := pending{scope: pair.ours.Scope, fan: fan}
	return &reaction{signal: querySignal(len(fan)), listing: listing(fan), question: &q}, true, nil
}

func (e *engine) compareExclusive(scope tree.Scope[[]byte]) (*reaction, bool, error) {
	survivor, err := e.filter(scope)
	if err != nil {
		return nil, true, err
	}
	if err := e.publish(survivor); err != nil {
		return nil, true, err
	}
	if survivor.Len() == 0 {
		return nil, true, nil
	}
	return &reaction{signal: protocol.SupplyContinue, supply: survivor}, true, nil
}

func querySignal(count int) protocol.Signal {
	if count == 0 {
		return protocol.QueryEmptyContinue
	}
	return protocol.QueryContinue
}

func (e *engine) emit(actions []reaction, depth int, stream uint8) error {
	for i := range actions {
		if actions[i].question != nil {
			actions[i].flushed = make(chan struct{})
		}
	}
	batch := replyBatch{actions: actions, stream: stream}
	select {
	case e.outputs[stream] <- batch:
	case <-e.ctx.Done():
		return e.ctx.Err()
	}
	for _, action := range actions {
		if err := e.publishQuestion(action, depth); err != nil {
			return err
		}
	}
	return nil
}

func (e *engine) publishQuestion(action reaction, depth int) error {
	if action.question == nil {
		return nil
	}
	select {
	case <-action.flushed:
	case <-e.ctx.Done():
		return e.ctx.Err()
	}
	return e.enqueue(depth+2, *action.question)
}

func (e *engine) emitAction(action reaction, stream uint8, last bool) error {
	if action.signal == protocol.SupplyContinue {
		return e.emitSupply(action.supply, stream, last)
	}
	signal := action.signal
	if last {
		signal++
	}
	if err := e.streams.send(
		stream,
		protocol.Frame{Stream: stream, Signal: signal, Listing: action.listing},
	); err != nil {
		return err
	}
	if action.flushed != nil {
		close(action.flushed)
	}
	return nil
}

func (e *engine) recordLimits() protocol.RecordLimits {
	return protocol.RecordLimits{
		MaxVersionBytes: e.remote.MaxVersionBytes,
	}
}

func (e *engine) emitSupply(scope tree.Scope[[]byte], stream uint8, last bool) error {
	target := min(e.local.TargetMessageSize, e.remote.TargetMessageSize)
	builder := protocol.NewRunBuilder(
		target,
		protocol.RecordLimits{MaxVersionBytes: e.local.MaxVersionBytes},
	)
	for version, payload := range scope.All() {
		record := protocol.Record{Version: version, Payload: payload}
		if err := e.pushRecord(builder, stream, record); err != nil {
			return err
		}
		e.sent.Add(1)
	}
	signal := protocol.SupplyContinue
	if last {
		signal = protocol.SupplyEnd
	}
	return e.streams.send(
		stream,
		protocol.Frame{Stream: stream, Signal: signal, Run: builder.Take()},
	)
}

func (e *engine) pushRecord(
	builder *protocol.RunBuilder,
	stream uint8,
	record protocol.Record,
) error {
	admitted, err := builder.Add(record)
	if err != nil {
		return err
	}
	if admitted {
		return nil
	}
	if err := e.streams.send(
		stream,
		protocol.Frame{Stream: stream, Signal: protocol.SupplyContinue, Run: builder.Take()},
	); err != nil {
		return err
	}
	admitted, err = builder.Add(record)
	if err != nil {
		return err
	}
	if !admitted {
		return errors.New("reconcile: empty supply builder refused record")
	}
	return nil
}

func exclusive(ours []protocol.Child, theirs []protocol.Child) []uint8 {
	var held [256]bool
	for _, child := range theirs {
		held[child.Radix] = true
	}
	var radices []uint8
	for _, child := range ours {
		if !held[child.Radix] {
			radices = append(radices, child.Radix)
		}
	}
	return radices
}

func (e *engine) sendEarly() error {
	radices := exclusive(e.local.Listing, e.remote.Listing)
	if len(radices) == 0 {
		return nil
	}
	var actions []reaction
	for _, radix := range radices {
		scope, _ := e.original.Scope([]byte{radix})
		survivor, err := e.filter(scope)
		if err != nil {
			return err
		}
		if err := e.publish(survivor); err != nil {
			return err
		}
		if survivor.Len() > 0 {
			actions = append(actions, reaction{signal: protocol.SupplyContinue, supply: survivor})
		}
	}
	if err := e.emit(actions, -1, 0); err != nil {
		return err
	}
	return nil
}

func (e *engine) earlySurvivor(scope tree.Scope[[]byte]) bool {
	if e.role != protocol.Initiator || scope.Depth() != 1 {
		return false
	}
	radix := scope.Prefix()[0]
	for _, child := range e.remote.Listing {
		if child.Radix == radix {
			return false
		}
	}
	return true
}
