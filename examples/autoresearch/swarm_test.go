//go:build unix

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// scripted research sees only local model context, never other replicas.
type scriptedResearch struct{ calls atomic.Int64 }

func (s *scriptedResearch) Respond(
	_ context.Context,
	actor string,
	input []json.RawMessage,
) (turn, error) {
	notes := visibleNotes(input)
	tools := researchStep(actor, notes)
	var response turn
	for _, tool := range tools {
		tool.Type = "function_call"
		tool.ID = fmt.Sprintf("call-%d", s.calls.Add(1))
		response.Calls = append(response.Calls, tool)
		response.Items = append(response.Items, jsonItem(tool))
	}
	return response, nil
}

func visibleNotes(input []json.RawMessage) []note {
	byID := make(map[string]note)
	for _, raw := range input {
		var item struct{ Role, Content string }
		if json.Unmarshal(raw, &item) != nil || item.Role != "user" {
			continue
		}
		var view struct {
			New []note `json:"new_messages"`
		}
		if json.Unmarshal([]byte(item.Content), &view) == nil {
			for _, n := range view.New {
				byID[n.ID] = n
			}
		}
	}
	notes := make([]note, 0, len(byID))
	for _, n := range byID {
		notes = append(notes, n)
	}
	return notes
}

func testTool(name string, args any) call {
	return call{Name: name, Arguments: string(jsonItem(args))}
}

func waitTool() []call { return []call{testTool("wait", map[string]int{"seconds": 1})} }

func researchStep(actor string, notes []note) []call {
	for _, n := range notes {
		if n.Author == actor && n.Kind == "status" && n.Exhausted {
			return waitTool()
		}
	}
	rest := testTool("status", map[string]any{"exhausted": true, "body": "no further ideas"})
	if actor == "agent-00" {
		return []call{
			testTool("try", map[string]string{"body": "measure the baseline"}),
			testTool("evaluate", map[string]string{"body": "baseline evidence"}), rest,
		}
	}
	if actor == "agent-01" {
		for _, n := range notes {
			if n.Author == "agent-00" && n.Kind == "experiment" {
				return []call{
					testTool("adopt", map[string]string{"ref": n.ID}),
					testTool(
						"try",
						map[string]string{"body": "independently reproduce peer evidence"},
					),
					testTool("evaluate", map[string]string{"body": "reproduced"}),
					rest,
				}
			}
		}
		return waitTool()
	}
	if actor == "agent-02" {
		return []call{
			testTool("try", map[string]string{"body": "a deliberately broken parser"}),
			testTool(
				"write_file",
				map[string]string{"path": "parser/parser.go", "content": "package parser\n"},
			),
			testTool(
				"evaluate",
				map[string]string{"body": "failed candidate remains in the notebook"},
			),
			rest,
		}
	}
	return []call{rest}
}

func testMembers() []string {
	members := make([]string, 10)
	for i := range members {
		members[i] = fmt.Sprintf("agent-%02d", i)
	}
	return members
}

func TestTenAgentsShareExperimentsThroughGossip(t *testing.T) {
	cfg := config{
		Agents:         10,
		Model:          "gpt-6-luna",
		Goal:           "share experiments, reproduce a peer candidate, and report exhaustion",
		Directory:      filepath.Join(t.TempDir(), "run"),
		Turns:          2000,
		CommandTimeout: 20 * time.Second,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var output bytes.Buffer
	if err := runSwarm(ctx, cfg, &scriptedResearch{}, &output); err != nil {
		t.Fatalf("%v\n%s", err, output.String())
	}
	var first []note
	for _, member := range testMembers() {
		data, err := os.ReadFile(filepath.Join(cfg.Directory, "boards", member+".json"))
		if err != nil {
			t.Fatal(err)
		}
		var notes []note
		if err := json.Unmarshal(data, &notes); err != nil {
			t.Fatal(err)
		}
		if !allExhausted(notes, testMembers()) {
			t.Fatalf("%s did not observe exhaustion", member)
		}
		experiments := 0
		for _, n := range notes {
			if n.Kind != "experiment" {
				continue
			}
			experiments++
			if n.Ref == "" || n.Measurement == nil || len(n.Files) == 0 {
				t.Fatal("experiment missing linked evidence")
			}
			if n.Author == "agent-02" && n.Result.Exit == 0 {
				t.Fatal("broken parser passed")
			}
			if n.Author != "agent-02" && len(n.Measurement.CandidateNS) != 5 {
				t.Fatalf("missing benchmark samples: %+v", n)
			}
		}
		if experiments != 3 {
			t.Fatalf("got %d experiments", experiments)
		}
		if first == nil {
			first = notes
		} else if !reflect.DeepEqual(first, notes) {
			t.Fatal("final gossip did not converge")
		}
	}
	for _, member := range []string{"agent-00", "agent-01"} {
		data, err := os.ReadFile(filepath.Join(cfg.Directory, member, "parser", "parser.go"))
		if err != nil || string(data) != baselineFiles()["parser/parser.go"] {
			t.Fatalf("artifact did not cross gossip into %s: %q %v", member, data, err)
		}
	}
	if _, err := os.Stat(filepath.Join(cfg.Directory, "winner", "main.go")); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "experiment logs converged") {
		t.Fatal("missing convergence outcome")
	}
}

type silentModel struct{}

func (silentModel) Respond(context.Context, string, []json.RawMessage) (turn, error) {
	return turn{}, nil
}

func TestLimitPreservesResearchWithoutClaimingExhaustion(t *testing.T) {
	cfg := config{
		Agents:         10,
		Directory:      filepath.Join(t.TempDir(), "run"),
		Turns:          1,
		CommandTimeout: 20 * time.Second,
	}
	var output bytes.Buffer
	err := runSwarm(context.Background(), cfg, silentModel{}, &output)
	if !errors.Is(err, errLimit) || strings.Contains(output.String(), "experiment logs converged") {
		t.Fatalf("limit reported as success: %v\n%s", err, output.String())
	}
}
