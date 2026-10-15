package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type research struct {
	Notes    []note
	Best     note
	Packages map[string]map[string]string
}

func loadResearch(cfg config) (research, error) {
	var saved research
	if cfg.Resume == "" {
		return saved, nil
	}
	data, err := os.ReadFile(filepath.Join(cfg.Resume, "run.json"))
	if err != nil {
		return saved, err
	}
	var previous config
	if err := json.Unmarshal(data, &previous); err != nil {
		return saved, err
	}
	if previous.Agents != cfg.Agents {
		return saved, errors.New("resuming requires the same number of agents")
	}
	saved.Notes, err = readNotebook(filepath.Join(cfg.Resume, "events.jsonl"))
	if err != nil {
		return saved, err
	}
	data, err = os.ReadFile(filepath.Join(cfg.Resume, "winner", "selection.json"))
	if err != nil {
		return saved, err
	}
	if err := json.Unmarshal(data, &saved.Best); err != nil {
		return saved, err
	}
	if saved.Best.Measurement == nil || saved.Best.Measurement.Workload != workloadHash() {
		saved.Best = note{}
	}
	saved.Packages, err = readPackages(cfg.Resume, cfg.Agents)
	return saved, err
}

func readNotebook(path string) ([]note, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	decoder := json.NewDecoder(file)
	var notes []note
	for {
		var n note
		if err := decoder.Decode(&n); err != nil {
			if errors.Is(err, io.EOF) {
				return notes, nil
			}
			return nil, err
		}
		notes = append(notes, n)
	}
}

func readPackages(directory string, count int) (map[string]map[string]string, error) {
	packages := make(map[string]map[string]string)
	for i := range count {
		id := fmt.Sprintf("agent-%02d", i)
		root, err := os.OpenRoot(filepath.Join(directory, id))
		if err != nil {
			return nil, err
		}
		a := &agent{root: root}
		files, err := a.candidateFiles()
		_ = root.Close()
		if err != nil {
			return nil, err
		}
		packages[id] = files
	}
	return packages, nil
}

func (r research) replay(n *network, events io.Writer) error {
	if err := n.peers[0].SendAll(r.Notes); err != nil {
		return err
	}
	encoder := json.NewEncoder(events)
	for _, entry := range r.Notes {
		if err := encoder.Encode(entry); err != nil {
			return err
		}
	}
	return nil
}

func (a *agent) restore(notes []note) {
	for _, n := range notes {
		if n.Author != a.id {
			continue
		}
		a.seq = max(a.seq, n.Seq)
		switch n.Kind {
		case "trying":
			a.active = n.ID
		case "experiment":
			if n.Ref == a.active {
				a.active = ""
			}
		}
	}
}
