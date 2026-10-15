package main

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/cirocosta/hearsay"
)

type note struct {
	ID          string            `json:"id"`
	Author      string            `json:"author"`
	Seq         int               `json:"seq"`
	Kind        string            `json:"kind"`
	Ref         string            `json:"ref,omitempty"`
	Exhausted   bool              `json:"exhausted,omitempty"`
	Measurement *measurement      `json:"measurement,omitempty"`
	Result      *commandResult    `json:"result,omitempty"`
	Body        string            `json:"body,omitempty"`
	Files       map[string]string `json:"files,omitempty"`
}

func readBoard(replica *hearsay.Replica[note]) ([]note, error) {
	var notes []note
	for _, message := range replica.Snapshot().Causal() {
		if message.Err != nil {
			return nil, message.Err
		}
		notes = append(notes, message.Payload)
	}
	return notes, nil
}

func findNote(notes []note, id string) (note, error) {
	for _, n := range notes {
		if n.ID == id {
			return n, nil
		}
	}
	return note{}, fmt.Errorf("message %q is not in your local replica yet", id)
}

func allExhausted(notes []note, members []string) bool {
	latest := make(map[string]note)
	for _, n := range notes {
		if n.Kind == "status" && n.Seq > latest[n.Author].Seq {
			latest[n.Author] = n
		}
	}
	for _, member := range members {
		if !latest[member].Exhausted {
			return false
		}
	}
	return len(members) > 0
}

func boardDelta(notes []note, seen map[string]bool, limits string) string {
	var fresh []note
	for _, n := range notes {
		if !seen[n.ID] {
			seen[n.ID] = true
			fresh = append(fresh, summarize(n))
		}
	}
	view := struct {
		New []note `json:"new_messages"`

		Limits string `json:"operational_limits"`
	}{New: fresh, Limits: limits}
	data, _ := json.Marshal(view)
	return string(data)
}

func summarize(n note) note {
	if n.Files != nil {
		var names []string
		for path := range n.Files {
			names = append(names, path)
		}
		sort.Strings(names)
		n.Body += "\nShared files (use adopt to read): " + strings.Join(names, ", ")
		n.Files = nil
	}
	if len(n.Body) > 12<<10 {
		n.Body = n.Body[:12<<10] + "\n[message preview truncated]"
	}
	return n
}
