package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
)

type record struct {
	Version string `json:"version"`
	Payload string `json:"payload"`
}
type response struct {
	OK      bool     `json:"ok"`
	Error   string   `json:"error"`
	Address string   `json:"address"`
	Version string   `json:"version"`
	Records []record `json:"records"`
	Status  string   `json:"status"`
}
type rust struct {
	cmd     *exec.Cmd
	input   io.WriteCloser
	output  *bufio.Scanner
	address string
}

func startRust(mode string) (*rust, error) {
	binary := os.Getenv("HEARSAY_INTEROP_RUST_BIN")
	if binary == "" {
		binary = ".agi.local/interop-target/debug/rumors-interop"
	}
	cmd := exec.Command(binary, mode)
	input, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	output, err := cmd.StdoutPipe()
	if err != nil {
		_ = input.Close()
		return nil, err
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		_ = input.Close()
		return nil, err
	}
	r := &rust{cmd: cmd, input: input, output: bufio.NewScanner(output)}
	r.output.Buffer(make([]byte, 4096), 64<<20)
	greeting, err := r.read()
	if err != nil {
		r.close()
		return nil, err
	}
	r.address = greeting.Address
	return r, nil
}
func (r *rust) close() { _ = r.input.Close(); _ = r.cmd.Wait() }
func (r *rust) read() (response, error) {
	var result response
	if !r.output.Scan() {
		return result, fmt.Errorf("rust stdout closed: %v", r.output.Err())
	}
	if err := json.Unmarshal(r.output.Bytes(), &result); err != nil {
		return result, err
	}
	if !result.OK {
		return result, fmt.Errorf("rust: %s", result.Error)
	}
	return result, nil
}

func (r *rust) command(command any) (response, error) {
	if err := json.NewEncoder(r.input).Encode(command); err != nil {
		return response{}, err
	}
	return r.read()
}
func (r *rust) op(op string) (response, error) { return r.command(map[string]any{"op": op}) }
func (r *rust) pair(op string, goFn func() error) error {
	return exchange(func() error { _, err := r.op(op); return err }, goFn)
}

func (r *rust) retirement(expected string, goFn func() error) error {
	return exchange(func() error {
		result, err := r.op("retire")
		if err != nil {
			return err
		}
		if result.Status != expected {
			return fmt.Errorf("rust retirement status %s; want %s", result.Status, expected)
		}
		return nil
	}, goFn)
}

func exchange(rustFn, goFn func() error) error {
	done := make(chan error, 1)
	go func() { done <- rustFn() }()
	goErr := goFn()
	rustErr := <-done
	return errors.Join(goErr, rustErr)
}
