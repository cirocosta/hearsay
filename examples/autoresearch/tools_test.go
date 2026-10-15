package main

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"

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
	root, err := os.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = replica.Close(); _ = root.Close() })
	return &agent{
		id:   "agent-00",
		peer: replica,
		root: root,
		log:  &logger{output: io.Discard, events: io.Discard},
	}
}

func TestUnanimityRejectsMissingOutsiderAndRevokedVotes(t *testing.T) {
	members := []string{"a", "b", "c"}
	notes := []note{
		{ID: "p", Kind: "proposal", Topic: "stop"},
		{Author: "a", Seq: 1, Kind: "vote", Ref: "p", Agree: true},
		{Author: "b", Seq: 1, Kind: "vote", Ref: "p", Agree: true},
		{Author: "outsider", Seq: 1, Kind: "vote", Ref: "p", Agree: true},
	}
	if stopAgreement(notes, members) != "" {
		t.Fatal("missing member was replaced by outsider")
	}
	notes = append(notes, note{Author: "c", Seq: 1, Kind: "vote", Ref: "p", Agree: true})
	if stopAgreement(notes, members) != "p" {
		t.Fatal("full agreement was not recognized")
	}
	notes = append(notes, note{Author: "b", Seq: 2, Kind: "vote", Ref: "p", Agree: false})
	if stopAgreement(notes, members) != "" {
		t.Fatal("earlier vote hid a later rejection")
	}
}

func TestStopVoteCanBeCorrectedAndAuthorIsLocal(t *testing.T) {
	a := testAgent(t)
	first, _ := a.publish(note{Kind: "proposal", Topic: "stop"})
	other, _ := a.publish(note{Kind: "proposal", Topic: "stop"})
	if _, err := a.vote(arguments{Ref: first, Agree: true}); err != nil {
		t.Fatal(err)
	}
	for _, vote := range []arguments{{Ref: first, Agree: false}, {Ref: other, Agree: true}} {
		if _, err := a.vote(vote); err != nil {
			t.Fatal(err)
		}
	}
	updated, _ := readBoard(a.peer)
	if voteCount(updated, first, []string{a.id}) != 0 ||
		voteCount(updated, other, []string{a.id}) != 1 {
		t.Fatal("topic vote did not replace prior endorsement")
	}
	if _, err := a.publish(note{Kind: "note", Author: "spoofed"}); err != nil {
		t.Fatal(err)
	}
	notes, _ := readBoard(a.peer)
	for _, n := range notes {
		if n.Author != a.id {
			t.Fatal("agent could impersonate another member")
		}
	}
}

func TestArtifactCopiesAndFileBoundaries(t *testing.T) {
	a := testAgent(t)
	if err := a.writeFile("src/candidate.s", "original"); err != nil {
		t.Fatal(err)
	}
	id, err := a.share("src/candidate.s", "candidate")
	if err != nil {
		t.Fatal(err)
	}
	if err := a.writeFile("src/candidate.s", "changed"); err != nil {
		t.Fatal(err)
	}
	if _, err := a.adopt(id); err != nil {
		t.Fatal(err)
	}
	text, err := a.readFile("src/candidate.s")
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
	if err := checkFinalAgreement(nil, []*agent{{finished: "a"}, {finished: "b"}}); err == nil {
		t.Fatal("different locally observed stop proposals became success")
	}
}

func TestBinaryFilesAreNotSilentlyChangedByJSON(t *testing.T) {
	a := testAgent(t)
	if err := a.root.WriteFile("object.o", []byte{0xff, 0xfe}, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := a.share("object.o", "binary"); err == nil {
		t.Fatal("binary artifact passed through a UTF-8 codec")
	}
	if err := a.writeFile("object.o", string([]byte{0xff})); err == nil {
		t.Fatal("file tool accepted non-UTF-8 content")
	}
	before, _ := a.root.ReadFile("object.o")
	if !reflect.DeepEqual(before, []byte{0xff, 0xfe}) {
		t.Fatal("rejected write changed the file")
	}
}
