package main

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/cirocosta/hearsay"
)

func testAgent(t *testing.T) *agent {
	t.Helper()
	peer, err := hearsay.Seed(hearsay.JSONCodec[note]())
	if err != nil {
		t.Fatal(err)
	}
	replica, err := peer.IntoReplica()
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	root, err := os.OpenRoot(directory)
	if err != nil {
		t.Fatal(err)
	}
	if err := seedWorkspace(root, baselineFiles(), baselineFiles()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = replica.Close(); _ = root.Close() })
	return &agent{
		id:   "agent-00",
		peer: replica,
		root: root,
		log:  &logger{output: io.Discard, events: io.Discard},
		executor: &executor{
			lane:      make(chan struct{}, 1),
			timeout:   20 * time.Second,
			directory: directory,
		},
	}
}

func TestExhaustionIsRevisableAndRequiresEveryMember(t *testing.T) {
	members := []string{"a", "b", "c"}
	notes := []note{
		{Author: "a", Seq: 1, Kind: "status", Exhausted: true},
		{Author: "b", Seq: 1, Kind: "status", Exhausted: true},
		{Author: "outsider", Seq: 1, Kind: "status", Exhausted: true},
	}
	if allExhausted(notes, members) {
		t.Fatal("outsider replaced a missing member")
	}
	notes = append(notes, note{Author: "c", Seq: 1, Kind: "status", Exhausted: true})
	if !allExhausted(notes, members) {
		t.Fatal("exhaustion not recognized")
	}
	notes = append(notes, note{Author: "b", Seq: 2, Kind: "status"})
	if allExhausted(notes, members) {
		t.Fatal("old exhaustion hid renewed research")
	}
}

func TestTryingClearsExhaustionAndAuthorIsLocal(t *testing.T) {
	a := testAgent(t)
	_, _ = a.publish(note{Kind: "status", Exhausted: true})
	if _, err := a.beginExperiment("try a different layout"); err != nil {
		t.Fatal(err)
	}
	if _, err := a.beginExperiment("another attempt"); err == nil {
		t.Fatal("overlapping experiment accepted")
	}
	if _, err := a.use(
		context.Background(),
		testTool("status", map[string]any{"exhausted": true, "body": "done"}),
	); err == nil {
		t.Fatal("unfinished experiment declared exhausted")
	}
	_, _ = a.publish(note{Kind: "note", Author: "spoofed"})
	notes, _ := readBoard(a.peer)
	if allExhausted(notes, []string{a.id}) {
		t.Fatal("trying did not clear exhaustion")
	}
	for _, n := range notes {
		if n.Author != a.id {
			t.Fatal("spoofed author")
		}
	}
}

func TestArtifactCopiesAndFileBoundaries(t *testing.T) {
	a := testAgent(t)
	if err := a.writeFile("parser/candidate.s", "original"); err != nil {
		t.Fatal(err)
	}
	id, err := a.publish(
		note{Kind: "experiment", Files: map[string]string{"parser/candidate.s": "original"}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.writeFile("parser/candidate.s", "changed"); err != nil {
		t.Fatal(err)
	}
	if _, err := a.adopt(id); err != nil {
		t.Fatal(err)
	}
	text, err := a.readFile("parser/candidate.s")
	if err != nil || text != "original" {
		t.Fatal("shared artifact changed with its source file")
	}
	for _, path := range []string{"../escape", filepath.Join(t.TempDir(), "absolute")} {
		if err := a.writeFile(path, "bad"); err == nil {
			t.Fatalf("accepted path %q", path)
		}
	}
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(a.root.Name(), "escape")); err != nil {
		t.Skip("symlinks unavailable")
	}
	if err := a.writeFile("escape/outside", "bad"); err == nil {
		t.Fatal("file tool followed a symlink outside its root")
	}
}

func TestToolArgumentsAreValidatedBeforeExecution(t *testing.T) {
	a := testAgent(t)
	for _, arguments := range []string{`{}`, `{"cmd":"echo wrong field"}`, `{"command":null}`, `{"command":""}`} {
		if _, err := a.use(
			context.Background(),
			call{Name: "run", Arguments: arguments},
		); err == nil {
			t.Fatalf("accepted malformed run: %s", arguments)
		}
	}
}

func TestBinaryFilesAreNotSilentlyChangedByJSON(t *testing.T) {
	a := testAgent(t)
	if err := a.root.WriteFile("parser/binary.go", []byte{0xff, 0xfe}, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := a.candidateFiles(); err == nil {
		t.Fatal("binary artifact passed through a UTF-8 codec")
	}
	if err := a.writeFile("parser/binary.go", string([]byte{0xff})); err == nil {
		t.Fatal("file tool accepted non-UTF-8 content")
	}
	before, _ := a.root.ReadFile("parser/binary.go")
	if !reflect.DeepEqual(before, []byte{0xff, 0xfe}) {
		t.Fatal("rejected write changed the file")
	}
}
