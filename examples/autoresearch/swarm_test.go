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

// scriptedResearch sees only each agent's model context, never other replicas
// or directories. The negotiation script is a test double, not production policy.
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

func proposal(notes []note, topic string) note {
	for _, n := range notes {
		if n.Kind == "proposal" && n.Topic == topic {
			return n
		}
	}
	return note{}
}

func voted(notes []note, actor, ref string) bool {
	for _, n := range notes {
		if n.Kind == "vote" && n.Author == actor && n.Ref == ref {
			return true
		}
	}
	return false
}

func researchStep(actor string, notes []note) []call {
	first := proposal(notes, "plan-draft")
	if first.ID == "" && actor == "agent-00" {
		return []call{
			testTool(
				"propose",
				map[string]string{"topic": "plan-draft", "body": "optimize immediately"},
			),
		}
	}
	if first.ID != "" && actor == "agent-05" && !voted(notes, actor, first.ID) {
		return []call{
			testTool(
				"vote",
				map[string]any{"ref": first.ID, "agree": false, "body": "agree correctness first"},
			),
		}
	}
	return revisedResearchStep(actor, notes, first)
}

func revisedResearchStep(actor string, notes []note, first note) []call {
	plan := proposal(notes, "plan")
	if plan.ID == "" {
		if actor == "agent-00" && voted(notes, "agent-05", first.ID) {
			return []call{
				testTool(
					"propose",
					map[string]string{
						"topic": "plan",
						"body":  "agree checks, share source, independently reproduce, then stop",
					},
				),
			}
		}
		return waitTool()
	}
	if !voted(notes, actor, plan.ID) {
		return []call{
			testTool(
				"vote",
				map[string]any{"ref": plan.ID, "agree": true, "body": "accept revised plan"},
			),
		}
	}
	if voteCount(notes, plan.ID, testMembers()) != 10 {
		return waitTool()
	}
	return evaluationStep(actor, notes)
}

func evaluationStep(actor string, notes []note) []call {
	evaluation := proposal(notes, "evaluation")
	if evaluation.ID == "" {
		if actor == "agent-01" {
			return []call{
				testTool(
					"propose",
					map[string]string{
						"topic": "evaluation",
						"body":  "two independent file checks; this test makes no speed claim",
					},
				),
			}
		}
		return waitTool()
	}
	if !voted(notes, actor, evaluation.ID) {
		return []call{
			testTool(
				"vote",
				map[string]any{"ref": evaluation.ID, "agree": true, "body": "accept evaluation"},
			),
		}
	}
	if voteCount(notes, evaluation.ID, testMembers()) != 10 {
		return waitTool()
	}
	return experimentStep(actor, notes)
}

func experimentStep(actor string, notes []note) []call {
	artifact := note{}
	results := make(map[string]bool)
	for _, n := range notes {
		if n.Kind == "artifact" {
			artifact = n
		}
		if n.Kind == "result" {
			results[n.Author] = true
		}
	}
	if actor == "agent-02" && artifact.ID == "" {
		return []call{
			testTool(
				"write_file",
				map[string]string{"path": "candidate.go", "content": "package parser\n"},
			),
			testTool(
				"run",
				map[string]string{"command": "test -s candidate.go && printf 'checked\\n'"},
			),
			testTool(
				"share",
				map[string]string{
					"paths": "candidate.go",
					"body":  "candidate for independent reproduction",
				},
			),
		}
	}
	if actor == "agent-03" && artifact.ID != "" && !results[actor] {
		return []call{
			testTool("adopt", map[string]string{"ref": artifact.ID}),
			testTool(
				"run",
				map[string]string{"command": "test -s candidate.go && printf 'replicated\\n'"},
			),
		}
	}
	return finishResearchStep(actor, notes, results)
}

func finishResearchStep(actor string, notes []note, results map[string]bool) []call {
	stop := proposal(notes, "stop")
	if stop.ID == "" && actor == "agent-04" && results["agent-02"] && results["agent-03"] {
		return []call{
			testTool(
				"propose",
				map[string]string{
					"topic": "stop",
					"body":  "shared artifact independently checked; agreed test goal complete",
				},
			),
		}
	}
	if stop.ID != "" && !voted(notes, actor, stop.ID) {
		return []call{
			testTool(
				"vote",
				map[string]any{"ref": stop.ID, "agree": true, "body": "checks complete"},
			),
		}
	}
	return waitTool()
}

func testMembers() []string {
	members := make([]string, 10)
	for i := range members {
		members[i] = fmt.Sprintf("agent-%02d", i)
	}
	return members
}

func TestTenAgentsNegotiateThroughGossip(t *testing.T) {
	cfg := config{
		Agents:         10,
		Model:          "gpt-6-luna",
		Goal:           "negotiate, exchange an artifact, reproduce, and agree to stop",
		Directory:      filepath.Join(t.TempDir(), "run"),
		Turns:          2000,
		CommandTimeout: time.Second,
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
		if stopAgreement(notes, testMembers()) == "" || len(agreements(notes, testMembers())) != 3 {
			t.Fatalf("%s did not observe all three agreements", member)
		}
		if first == nil {
			first = notes
		} else if !reflect.DeepEqual(first, notes) {
			t.Fatal("final gossip did not converge")
		}
	}
	for _, member := range []string{"agent-02", "agent-03"} {
		data, err := os.ReadFile(filepath.Join(cfg.Directory, member, "candidate.go"))
		if err != nil || string(data) != "package parser\n" {
			t.Fatalf("artifact did not cross gossip into %s: %q %v", member, data, err)
		}
	}
	if !strings.Contains(output.String(), "boards converged") {
		t.Fatal("missing convergence outcome")
	}
}

type silentModel struct{}

func (silentModel) Respond(context.Context, string, []json.RawMessage) (turn, error) {
	return turn{}, nil
}

func TestLimitIsNotAgreement(t *testing.T) {
	cfg := config{
		Agents:         10,
		Directory:      filepath.Join(t.TempDir(), "run"),
		Turns:          1,
		CommandTimeout: time.Second,
	}
	var output bytes.Buffer
	err := runSwarm(context.Background(), cfg, silentModel{}, &output)
	if !errors.Is(err, errLimit) || strings.Contains(output.String(), "boards converged") {
		t.Fatalf("limit reported as success: %v\n%s", err, output.String())
	}
}
