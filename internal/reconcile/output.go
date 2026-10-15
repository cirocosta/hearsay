package reconcile

import "github.com/cirocosta/hearsay/internal/protocol"

type replyBatch struct {
	actions []reaction
	stream  uint8
}

func (e *engine) startOutputs() {
	for stream := uint8(0); stream < 17; stream++ {
		e.startOutput(stream)
	}
}

func (e *engine) startOutput(stream uint8) {
	e.outputs[stream] = make(chan replyBatch, 1)
	e.outputDone[stream] = make(chan struct{})
	e.launch(func() error { defer close(e.outputDone[stream]); return e.output(stream) })
}

func (e *engine) output(stream uint8) error {
	for {
		select {
		case <-e.ctx.Done():
			return e.ctx.Err()
		case batch, ok := <-e.outputs[stream]:
			if !ok {
				return e.streams.finishSend(stream)
			}
			if err := e.writeReply(batch); err != nil {
				return err
			}
		}
	}
}

func (e *engine) writeReply(batch replyBatch) error {
	if len(batch.actions) == 0 {
		return e.streams.send(
			batch.stream,
			protocol.Frame{Stream: batch.stream, Signal: protocol.ReplyEnd},
		)
	}
	// writing must remain independent of publishing bounded local questions:
	// a conforming peer may expose a reply only after its final frame arrives.
	for i, action := range batch.actions {
		if err := e.emitAction(
			action,
			batch.stream,
			i == len(batch.actions)-1,
		); err != nil {
			return err
		}
	}
	return nil
}

func (e *engine) finishOutput(stream uint8) error {
	close(e.outputs[stream])
	select {
	case <-e.outputDone[stream]:
		return nil
	case <-e.ctx.Done():
		return e.ctx.Err()
	}
}
