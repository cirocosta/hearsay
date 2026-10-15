package main

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

//go:embed workspace
var workspace embed.FS

type measurement struct {
	Hash        string    `json:"hash"`
	Workload    string    `json:"workload"`
	CandidateNS []float64 `json:"candidate_ns"`
	ReferenceNS []float64 `json:"reference_ns"`
	Promoted    bool      `json:"promoted"`
}

func baselineFiles() map[string]string {
	data, _ := workspace.ReadFile("workspace/parser/parser.go.txt")
	return map[string]string{"parser/parser.go": string(data)}
}

func seedWorkspace(root *os.Root, files, reference map[string]string) error {
	for _, name := range []string{"go.mod", "main.go", "parser_test.go", "contract/contract.go"} {
		data, err := workspace.ReadFile("workspace/" + name + ".txt")
		if err != nil {
			return err
		}
		if err := writeWorkspaceFile(root, name, string(data)); err != nil {
			return err
		}
	}
	for target, source := range map[string]map[string]string{"oracle": baselineFiles(), "reference": reference, "parser": files} {
		if err := writePackage(root, target, source); err != nil {
			return err
		}
	}
	return nil
}

func writePackage(root *os.Root, target string, files map[string]string) error {
	for name, content := range files {
		name = target + "/" + filepath.Base(name)
		if err := writeWorkspaceFile(root, name, content); err != nil {
			return err
		}
	}
	return nil
}

func writeWorkspaceFile(root *os.Root, name, content string) error {
	if err := root.MkdirAll(filepath.Dir(name), 0o700); err != nil {
		return err
	}
	return root.WriteFile(name, []byte(content), 0o600)
}

func packageFile(name string) bool {
	if filepath.Dir(name) != "parser" || strings.HasSuffix(name, "_test.go") {
		return false
	}
	switch filepath.Ext(name) {
	case ".go", ".s", ".h":
		return true
	default:
		return false
	}
}

func (a *agent) candidateFiles() (map[string]string, error) {
	entries, err := fs.ReadDir(a.root.FS(), "parser")
	if err != nil {
		return nil, err
	}
	files := make(map[string]string)
	total := 0
	for _, entry := range entries {
		name := "parser/" + entry.Name()
		if entry.IsDir() || !packageFile(name) {
			continue
		}
		content, err := a.readFile(name)
		if err != nil {
			return nil, err
		}
		total += len(content)
		if total > 256<<10 || len(files) >= 32 {
			return nil, errors.New("candidate exceeds 256 KiB or 32 files")
		}
		files[name] = content
	}
	return files, nil
}

func filesHash(files map[string]string) string {
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)
	hash := sha256.New()
	for _, name := range names {
		_, _ = fmt.Fprintf(hash, "%d:%s%d:%s", len(name), name, len(files[name]), files[name])
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func workloadHash() string {
	test, _ := workspace.ReadFile("workspace/parser_test.go.txt")
	contract, _ := workspace.ReadFile("workspace/contract/contract.go.txt")
	return filesHash(map[string]string{"tests": string(test), "contract": string(contract)})
}

func (a *agent) evaluate(ctx context.Context, body string) (string, error) {
	if a.active == "" {
		return "", errors.New("announce an experiment with try before evaluating")
	}
	files, err := a.candidateFiles()
	if err != nil {
		return "", err
	}
	e := a.executor
	select {
	case e.lane <- struct{}{}:
		defer func() { <-e.lane }()
	case <-ctx.Done():
		return "", ctx.Err()
	}
	result, m, err := e.checkCandidate(ctx, files)
	if err != nil {
		return "", err
	}
	n := note{
		Kind:        "experiment",
		Ref:         a.active,
		Body:        body,
		Files:       files,
		Measurement: m,
		Result:      &result,
	}
	id, err := a.publish(n)
	if err != nil {
		return "", err
	}
	a.active = ""
	if m.Promoted {
		n.ID, n.Author = id, a.id
		e.best = n
	}
	return fmt.Sprintf("experiment %s\n%s", id, jsonItem(summarize(n))), nil
}

func (e *executor) checkCandidate(
	ctx context.Context,
	files map[string]string,
) (commandResult, *measurement, error) {
	directory, err := os.MkdirTemp(e.directory, ".evaluate-")
	if err != nil {
		return commandResult{}, nil, err
	}
	defer func() { _ = os.RemoveAll(directory) }()
	root, err := os.OpenRoot(directory)
	if err != nil {
		return commandResult{}, nil, err
	}
	defer func() { _ = root.Close() }()
	reference := baselineFiles()
	if e.best.Files != nil {
		reference = e.best.Files
	}
	if err := seedWorkspace(root, files, reference); err != nil {
		return commandResult{}, nil, err
	}
	bounded, cancel := context.WithTimeout(ctx, e.timeout)
	defer cancel()
	result, runErr := executeCommand(
		bounded,
		directory,
		"go test ./... && go test -run='^$' -bench='^Benchmark(Candidate|Reference)$' -benchmem -benchtime=100ms -count=5 .",
	)
	m := &measurement{Hash: filesHash(files), Workload: workloadHash()}
	if runErr != nil {
		result.Exit = -1
		result.Output += "\nrunner error: " + runErr.Error()
	}
	if result.Exit == 0 && !result.Truncated {
		m.CandidateNS, m.ReferenceNS = benchmarkSamples(result.Output)
		m.Promoted = m.Hash != filesHash(reference) && improves(m.CandidateNS, m.ReferenceNS)
	}
	return result, m, nil
}

func benchmarkSamples(output string) (candidate, reference []float64) {
	for line := range strings.SplitSeq(output, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 4 || fields[3] != "ns/op" {
			continue
		}
		value, err := strconv.ParseFloat(fields[2], 64)
		if err != nil || value <= 0 {
			continue
		}
		switch {
		case strings.HasPrefix(fields[0], "BenchmarkCandidate-"):
			candidate = append(candidate, value)
		case strings.HasPrefix(fields[0], "BenchmarkReference-"):
			reference = append(reference, value)
		}
	}
	return candidate, reference
}

func improves(candidate, reference []float64) bool {
	if len(candidate) != 5 || len(reference) != 5 {
		return false
	}
	a, b := append([]float64(nil), candidate...), append([]float64(nil), reference...)
	sort.Float64s(a)
	sort.Float64s(b)
	return a[2] < b[2]*0.97
}

func saveWinner(directory string, best note) error {
	directory = filepath.Join(directory, "winner")
	if err := os.Mkdir(directory, 0o700); err != nil {
		return err
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		return err
	}
	defer func() { _ = root.Close() }()
	files := best.Files
	if files == nil {
		files = baselineFiles()
		best = note{
			Kind: "baseline",
			Body: "no candidate cleared the correctness and improvement gates",
		}
	}
	if err := seedWorkspace(root, files, baselineFiles()); err != nil {
		return err
	}
	return root.WriteFile("selection.json", jsonItem(best), 0o600)
}
