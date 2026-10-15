package main

import (
	"context"
	"encoding/json"

	"github.com/cirocosta/hearsay/examples/autoresearch/internal/codex"
)

type (
	call = codex.Call
	turn = codex.Turn
)

type model interface {
	Respond(context.Context, string, []json.RawMessage) (turn, error)
}

type codexModel struct{ client *codex.Client }

func (m codexModel) Respond(
	ctx context.Context,
	actor string,
	input []json.RawMessage,
) (turn, error) {
	return m.client.Respond(ctx, codex.Request{
		Actor: actor, Instructions: instructions(actor), Input: input, Tools: toolDefinitions(),
	})
}

func jsonItem(v any) json.RawMessage {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}

func userItem(text string) json.RawMessage {
	return jsonItem(map[string]any{"role": "user", "content": text})
}
