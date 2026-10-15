package main

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/cirocosta/hearsay"
)

type note struct {
	ID     string            `json:"id"`
	Author string            `json:"author"`
	Seq    int               `json:"seq"`
	Kind   string            `json:"kind"`
	Topic  string            `json:"topic,omitempty"`
	Ref    string            `json:"ref,omitempty"`
	Agree  bool              `json:"agree,omitempty"`
	Body   string            `json:"body,omitempty"`
	Files  map[string]string `json:"files,omitempty"`
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

func agreements(notes []note, members []string) []note {
	var agreed []note
	for _, n := range notes {
		if n.Kind == "proposal" && voteCount(notes, n.ID, members) == len(members) {
			agreed = append(agreed, n)
		}
	}
	return agreed
}

func voteCount(notes []note, proposal string, members []string) int {
	topics := proposalTopics(notes)
	latest := make(map[string]note)
	for _, n := range notes {
		if n.Kind == "vote" && topics[n.Ref] == topics[proposal] && n.Seq > latest[n.Author].Seq {
			latest[n.Author] = n
		}
	}
	count := 0
	for _, member := range members {
		if latest[member].Agree && latest[member].Ref == proposal {
			count++
		}
	}
	return count
}

func proposalTopics(notes []note) map[string]string {
	topics := make(map[string]string)
	for _, n := range notes {
		if n.Kind == "proposal" {
			topics[n.ID] = n.Topic
		}
	}
	return topics
}

func stopAgreement(notes []note, members []string) string {
	for _, n := range agreements(notes, members) {
		if n.Topic == "stop" {
			return n.ID
		}
	}
	return ""
}

func boardDelta(notes []note, seen map[string]bool, members []string, limits string) string {
	var fresh []note
	for _, n := range notes {
		if !seen[n.ID] {
			seen[n.ID] = true
			fresh = append(fresh, summarize(n))
		}
	}
	view := struct {
		New    []note `json:"new_messages"`
		Agreed []note `json:"unanimous_proposals"`
		Limits string `json:"operational_limits"`
	}{New: fresh, Agreed: agreements(notes, members), Limits: limits}
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
