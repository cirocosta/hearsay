package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/cirocosta/hearsay/examples/autoresearch/internal/codex"
)

type toolDefinition = codex.Tool

func defineTool(name, description string, fields map[string]string) toolDefinition {
	properties := make(map[string]any)
	required := make([]string, 0, len(fields))
	for key, kind := range fields {
		properties[key] = map[string]string{"type": kind}
		required = append(required, key)
	}
	sort.Strings(required)
	return toolDefinition{
		Type: "function", Name: name, Description: description,
		Parameters: map[string]any{
			"type": "object", "properties": properties, "required": required,
			"additionalProperties": false,
		}, Strict: true,
	}
}

func toolDefinitions() []toolDefinition {
	return []toolDefinition{
		defineTool(
			"post",
			"Share a hypothesis, question, evidence, objection, or progress note through Hearsay.",
			map[string]string{"body": "string"},
		),
		defineTool(
			"propose",
			"Propose a shared decision on any topic. Topic stop proposes ending the run. No automatic self-vote.",
			map[string]string{"topic": "string", "body": "string"},
		),
		defineTool(
			"vote",
			"Agree/disagree with a visible proposal ID, explaining why. Your latest vote replaces your previous vote on that topic.",
			map[string]string{"ref": "string", "agree": "boolean", "body": "string"},
		),
		defineTool(
			"read_file",
			"Read a UTF-8 file in your research directory (at most 256 KiB).",
			map[string]string{"path": "string"},
		),
		defineTool(
			"write_file",
			"Create/replace a UTF-8 file in your research directory, including Go, C, or assembly source.",
			map[string]string{"path": "string", "content": "string"},
		),
		defineTool(
			"run",
			"Execute a shell command in your directory. All commands share one serialized lane; results are gossiped automatically. No background jobs or changes outside your directory.",
			map[string]string{"command": "string"},
		),
		defineTool(
			"share",
			"Publish source or data files through Hearsay. Paths are newline-separated, total at most 256 KiB. Body describes hypothesis and validation.",
			map[string]string{"paths": "string", "body": "string"},
		),
		defineTool(
			"adopt",
			"Import copies of files from a visible shared artifact ID into your own directory. May overwrite your local files.",
			map[string]string{"ref": "string"},
		),
		defineTool(
			"wait",
			"Wait for a local replica change or a timeout; seconds must be between 1 and 30.",
			map[string]string{"seconds": "integer"},
		),
	}
}

type arguments struct {
	Body    string `json:"body"`
	Topic   string `json:"topic"`
	Ref     string `json:"ref"`
	Agree   bool   `json:"agree"`
	Path    string `json:"path"`
	Paths   string `json:"paths"`
	Content string `json:"content"`
	Command string `json:"command"`
	Seconds int    `json:"seconds"`
}

func (a *agent) use(ctx context.Context, c call) (string, error) {
	var args arguments
	if err := decodeArguments(c, &args); err != nil {
		return "", err
	}
	switch c.Name {
	case "post":
		return a.publish(note{Kind: "note", Body: args.Body})
	case "propose":
		if args.Topic == "" || args.Body == "" {
			return "", errors.New("proposal needs a topic and body")
		}
		return a.publish(note{Kind: "proposal", Topic: args.Topic, Body: args.Body})
	case "vote":
		return a.vote(args)
	default:
		return a.workspaceTool(ctx, c.Name, args)
	}
}

func decodeArguments(c call, args *arguments) error {
	fields, err := toolFields(c.Name)
	if err != nil {
		return err
	}
	var values map[string]json.RawMessage
	if err := json.Unmarshal([]byte(c.Arguments), &values); err != nil {
		return err
	}
	if len(values) != len(fields) {
		return errors.New("tool arguments must match the declared fields")
	}
	for field := range fields {
		if value, ok := values[field]; !ok || string(value) == "null" {
			return fmt.Errorf("missing tool argument %q", field)
		}
	}
	return json.Unmarshal([]byte(c.Arguments), args)
}

func toolFields(name string) (map[string]any, error) {
	for _, tool := range toolDefinitions() {
		if tool.Name == name {
			return tool.Parameters["properties"].(map[string]any), nil
		}
	}
	return nil, fmt.Errorf("unknown tool %q", name)
}

func (a *agent) workspaceTool(ctx context.Context, name string, args arguments) (string, error) {
	switch name {
	case "read_file":
		return a.readFile(args.Path)
	case "write_file":
		return "written", a.writeFile(args.Path, args.Content)
	case "run":
		return a.runCommand(ctx, args.Command)
	case "share":
		return a.share(args.Paths, args.Body)
	case "adopt":
		return a.adopt(args.Ref)
	case "wait":
		return "replica checked; read the next board update", a.wait(ctx, args.Seconds)
	default:
		return "", fmt.Errorf("unknown tool %q", name)
	}
}

func (a *agent) vote(args arguments) (string, error) {
	notes, err := readBoard(a.peer)
	if err != nil {
		return "", err
	}
	proposal, err := findNote(notes, args.Ref)
	if err != nil {
		return "", err
	}
	if proposal.Kind != "proposal" {
		return "", errors.New("votes must reference a proposal")
	}
	id, err := a.publish(note{Kind: "vote", Ref: args.Ref, Agree: args.Agree, Body: args.Body})
	return id, err
}

func safePath(path string) error {
	if !filepath.IsLocal(path) || path == "." {
		return errors.New("path must be relative to your research directory")
	}
	return nil
}

func (a *agent) readFile(path string) (string, error) {
	if err := safePath(path); err != nil {
		return "", err
	}
	file, err := a.root.Open(path)
	if err != nil {
		return "", err
	}
	defer func() { _ = file.Close() }()
	data, err := io.ReadAll(io.LimitReader(file, (256<<10)+1))
	if len(data) > 256<<10 {
		return "", errors.New("file exceeds 256 KiB")
	}
	if !utf8.Valid(data) {
		return "", errors.New("file is not UTF-8; encode binary artifacts as text before sharing")
	}
	return string(data), err
}

func (a *agent) writeFile(path, content string) error {
	if err := safePath(path); err != nil {
		return err
	}
	if len(content) > 256<<10 {
		return errors.New("file exceeds 256 KiB")
	}
	if !utf8.ValidString(content) {
		return errors.New("file content must be UTF-8")
	}
	if err := a.root.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	return a.root.WriteFile(path, []byte(content), 0o600)
}

func (a *agent) share(paths, body string) (string, error) {
	files := make(map[string]string)
	size := 0
	for _, path := range strings.Split(paths, "\n") {
		path = strings.TrimSpace(path)
		if path == "" {
			continue
		}
		text, err := a.readFile(path)
		if err != nil {
			return "", err
		}
		size += len(text)
		if size > 256<<10 || len(files) >= 32 {
			return "", errors.New("artifact exceeds 256 KiB or 32 files")
		}
		files[path] = text
	}
	if len(files) == 0 {
		return "", errors.New("share needs at least one path")
	}
	return a.publish(note{Kind: "artifact", Body: body, Files: files})
}

func (a *agent) adopt(id string) (string, error) {
	notes, err := readBoard(a.peer)
	if err != nil {
		return "", err
	}
	artifact, err := findNote(notes, id)
	if err != nil {
		return "", err
	}
	if artifact.Kind != "artifact" {
		return "", errors.New("message is not a shared artifact")
	}
	for path := range artifact.Files {
		if err := safePath(path); err != nil {
			return "", err
		}
	}
	for path, content := range artifact.Files {
		if err := a.writeFile(path, content); err != nil {
			return "", err
		}
	}
	return fmt.Sprintf("adopted %d files from %s", len(artifact.Files), id), nil
}
