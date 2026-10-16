package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/cirocosta/hearsay"
)

func (n *node) startHTTP(ctx context.Context) error {
	if n.config.HTTP == "" {
		return nil
	}
	listener, err := net.Listen("tcp", n.config.HTTP)
	if err != nil {
		return fmt.Errorf("listen http: %w", err)
	}
	server := &http.Server{
		Handler:           n.handler(),
		ReadHeaderTimeout: n.config.IOTimeout,
		ReadTimeout:       n.config.IOTimeout,
		WriteTimeout:      n.config.IOTimeout,
		IdleTimeout:       30 * time.Second,
		BaseContext:       func(net.Listener) context.Context { return ctx },
	}
	n.mu.Lock()
	_, err = fmt.Fprintf(n.output, "http http://%s worker=%q\n", listener.Addr(), n.self.Name)
	n.mu.Unlock()
	if err != nil {
		_ = listener.Close() // startup has already failed; release the listener.
		return fmt.Errorf("print http address: %w", err)
	}
	n.appWorkers.Go(func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			n.mu.Lock()
			n.failure = fmt.Errorf("serve http: %w", err)
			n.mu.Unlock()
		}
	})
	n.appWorkers.Go(func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.WithoutCancel(ctx), n.config.IOTimeout)
		defer cancel()
		if err := server.Shutdown(shutdown); err != nil {
			_ = server.Close() // force remaining requests closed after the grace period.
		}
	})
	return nil
}

func (n *node) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /issues", n.submitIssue)
	mux.HandleFunc("GET /board", func(w http.ResponseWriter, _ *http.Request) {
		view, err := readBoard(n.replica)
		if err != nil {
			http.Error(w, "read board failed", http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, view)
	})
	mux.HandleFunc("GET /members", n.listMembers)
	return mux
}

func (n *node) submitIssue(w http.ResponseWriter, request *http.Request) {
	request.Body = http.MaxBytesReader(w, request.Body, 64<<10)
	issue, err := decodeIssue(request.Body)
	if err != nil {
		status := http.StatusBadRequest
		var oversized *http.MaxBytesError
		if errors.As(err, &oversized) {
			status = http.StatusRequestEntityTooLarge
		}
		http.Error(w, err.Error(), status)
		return
	}
	if strings.TrimSpace(issue.Title) == "" || len(issue.Title) > 256 {
		http.Error(
			w,
			"title must contain 1 to 256 bytes and cannot be blank",
			http.StatusBadRequest,
		)
		return
	}
	if request.Context().Err() != nil {
		return
	}
	value, err := n.addIssue(issue.Title, issue.Body)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, hearsay.ErrClosed) {
			status = http.StatusServiceUnavailable
		}
		http.Error(w, "submit issue failed", status)
		return
	}
	writeJSON(w, http.StatusCreated, value)
}

type issueInput struct {
	Title string `json:"title"`
	Body  string `json:"body"`
}

func decodeIssue(reader io.Reader) (issueInput, error) {
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	var issue issueInput
	if err := decoder.Decode(&issue); err != nil {
		return issueInput{}, err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("expected one JSON object")
		}
		return issueInput{}, err
	}
	return issue, nil
}

func (n *node) listMembers(w http.ResponseWriter, _ *http.Request) {
	type memberView struct {
		record
		Status string `json:"status"`
	}
	n.mu.Lock()
	view := struct {
		Self      record       `json:"self"`
		Neighbors []string     `json:"neighbors"`
		Members   []memberView `json:"members"`
	}{Self: n.self, Neighbors: []string{}, Members: []memberView{}}
	now := time.Now()
	for _, address := range sortedAddresses(n.members) {
		m := n.members[address]
		view.Members = append(
			view.Members,
			memberView{
				record: m.Record,
				Status: m.status(now, n.config.SuspectAfter, n.config.UnreachableAfter),
			},
		)
	}
	for address := range n.links {
		view.Neighbors = append(view.Neighbors, address)
	}
	n.mu.Unlock()
	slices.Sort(view.Neighbors)
	writeJSON(w, http.StatusOK, view)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value) // a disconnected HTTP client needs no retry.
}
