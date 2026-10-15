//go:build unix

package main

import (
	"context"
	"strings"
	"testing"
)

func TestEvaluationRestoresCommonChecks(t *testing.T) {
	a := testAgent(t)
	source := baselineFiles()["parser/parser.go"]
	source = strings.Replace(source,
		"func Parse(data []byte) (contract.Message, error) {",
		`func Parse(data []byte) (contract.Message, error) {
	if string(data) == "{\"channel\":\"trades\",\"data\":[],\"channel\":\"other\"}" {
		return contract.Message{}, nil
	}`, 1)
	if err := a.writeFile("parser/parser.go", source); err != nil {
		t.Fatal(err)
	}
	if err := a.root.WriteFile("parser_test.go", []byte("package main\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := a.beginExperiment("skip a duplicate channel check"); err != nil {
		t.Fatal(err)
	}
	id, err := a.evaluate(context.Background(), "this must fail despite the changed local tests")
	if err != nil {
		t.Fatal(err)
	}
	notes, _ := readBoard(a.peer)
	result, err := findNote(notes, strings.Split(id, "\n")[0][len("experiment "):])
	if err != nil {
		t.Fatal(err)
	}
	if result.Result.Exit == 0 || result.Measurement.Promoted ||
		!strings.Contains(result.Result.Output, "unsupported channel") {
		t.Fatalf("candidate escaped common checks: %+v", result)
	}
	if result.Files["parser/parser.go"] != source {
		t.Fatal("failure did not retain the evaluated source")
	}
}

func TestImprovementRequiresCompleteSamplesAndThreshold(t *testing.T) {
	reference := []float64{100, 100, 100, 100, 100}
	for _, candidate := range [][]float64{
		{90}, {98, 98, 98, 98, 98}, {1, 1, 100, 100, 100},
	} {
		if improves(candidate, reference) {
			t.Fatalf("incomplete or weak evidence promoted: %v", candidate)
		}
	}
	if !improves([]float64{95, 96, 94, 96, 95}, reference) {
		t.Fatal("repeatable improvement rejected")
	}
	output := "BenchmarkCandidate-12 100 95 ns/op 32 B/op 1 allocs/op\n" +
		"BenchmarkReference-12 100 100 ns/op 64 B/op 2 allocs/op\n"
	candidate, baseline := benchmarkSamples(output)
	if len(candidate) != 1 || candidate[0] != 95 || len(baseline) != 1 || baseline[0] != 100 {
		t.Fatal("benchmark samples not decoded")
	}
}
