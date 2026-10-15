package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type resumeModel struct{}

func (resumeModel) Respond(context.Context, string, []json.RawMessage) (turn, error) {
	return turn{}, nil
}

func TestResumeRetainsNotebookPackageAndPendingExperiment(t *testing.T) {
	cfg := config{
		Agents: 2, Directory: filepath.Join(t.TempDir(), "first"),
		Turns: 1, CommandTimeout: time.Second,
	}
	model := resumeModel{}
	if err := runSwarm(context.Background(), cfg, model, io.Discard); !errors.Is(err, errLimit) {
		t.Fatal(err)
	}
	source := baselineFiles()["parser/parser.go"] + "\n// retained research\n"
	path := filepath.Join(cfg.Directory, "agent-00", "parser", "parser.go")
	if err := os.WriteFile(path, []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}
	pending := note{
		ID:     "agent-00:7",
		Author: "agent-00",
		Seq:    7,
		Kind:   "trying",
		Body:   "unfinished idea",
	}
	if err := os.WriteFile(filepath.Join(cfg.Directory, "events.jsonl"),
		append(jsonItem(pending), '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	resumed := cfg
	resumed.Resume = cfg.Directory
	resumed.Directory = filepath.Join(t.TempDir(), "continued")
	prior, err := loadResearch(resumed)
	if err != nil {
		t.Fatal(err)
	}
	a := &agent{id: "agent-00"}
	a.restore(prior.Notes)
	if a.active != pending.ID || a.seq != 7 {
		t.Fatal("pending attempt or sequence lost")
	}
	if err := runSwarm(
		context.Background(),
		resumed,
		model,
		io.Discard,
	); !errors.Is(
		err,
		errLimit,
	) {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(resumed.Directory, "agent-00", "parser", "parser.go"))
	if err != nil || string(data) != source {
		t.Fatal("private parser was not retained")
	}
	notes, err := readNotebook(filepath.Join(resumed.Directory, "events.jsonl"))
	if err != nil || len(notes) != 1 || notes[0].ID != pending.ID {
		t.Fatal("notebook was not retained")
	}
}
