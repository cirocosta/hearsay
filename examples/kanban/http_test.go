package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestIssueAPI(t *testing.T) {
	n := boardNode(t)
	n.members[n.self.Address] = member{Record: n.self, Seen: time.Now()}
	handler := n.handler()
	request := httptest.NewRequest(
		http.MethodPost,
		"/issues",
		strings.NewReader(`{"title":"example issue","body":"copied from GitHub"}`),
	)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusCreated {
		t.Fatalf("POST /issues status = %d, body=%s", response.Code, response.Body)
	}
	var issue card
	if err := json.Unmarshal(response.Body.Bytes(), &issue); err != nil {
		t.Fatal(err)
	}
	if issue.ID == "" || issue.Title != "example issue" || issue.Body != "copied from GitHub" {
		t.Fatalf("submitted issue = %+v", issue)
	}
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/board", nil))
	var view board
	if err := json.Unmarshal(response.Body.Bytes(), &view); err != nil {
		t.Fatal(err)
	}
	if response.Code != http.StatusOK || len(view.Todo) != 1 || view.Todo[0].ID != issue.ID ||
		view.InProgress == nil ||
		view.Done == nil {
		t.Fatalf("GET /board = %+v, status=%d", view, response.Code)
	}
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/members", nil))
	var members struct {
		Self      record   `json:"self"`
		Neighbors []string `json:"neighbors"`
		Members   []struct {
			Address string `json:"address"`
			Status  string `json:"status"`
		} `json:"members"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &members); err != nil {
		t.Fatal(err)
	}
	if response.Code != http.StatusOK || members.Self.Incarnation != n.self.Incarnation ||
		len(members.Members) != 1 ||
		members.Members[0].Status != "active" ||
		members.Neighbors == nil {
		t.Fatalf("GET /members = %+v, status=%d", members, response.Code)
	}
}

func TestIssueAPIErrors(t *testing.T) {
	n := boardNode(t)
	handler := n.handler()
	for _, test := range []struct {
		Name   string
		Method string
		Path   string
		Body   string
		Status int
	}{
		{Name: "malformed", Method: http.MethodPost, Path: "/issues", Body: "{", Status: http.StatusBadRequest},
		{Name: "unknown_field", Method: http.MethodPost, Path: "/issues", Body: `{"title":"task","assignee":"self"}`, Status: http.StatusBadRequest},
		{Name: "blank_title", Method: http.MethodPost, Path: "/issues", Body: `{"title":"  "}`, Status: http.StatusBadRequest},
		{Name: "long_title", Method: http.MethodPost, Path: "/issues", Body: `{"title":"` + strings.Repeat("a", 257) + `"}`, Status: http.StatusBadRequest},
		{Name: "trailing_json", Method: http.MethodPost, Path: "/issues", Body: `{"title":"task"} {}`, Status: http.StatusBadRequest},
		{Name: "oversized", Method: http.MethodPost, Path: "/issues", Body: `{"title":"task","body":"` + strings.Repeat("a", 64<<10) + `"}`, Status: http.StatusRequestEntityTooLarge},
		{Name: "wrong_method", Method: http.MethodDelete, Path: "/issues", Status: http.StatusMethodNotAllowed},
		{Name: "unknown_route", Method: http.MethodGet, Path: "/missing", Status: http.StatusNotFound},
	} {
		t.Run(test.Name, func(t *testing.T) {
			response := httptest.NewRecorder()
			handler.ServeHTTP(
				response,
				httptest.NewRequest(test.Method, test.Path, strings.NewReader(test.Body)),
			)
			if response.Code != test.Status {
				t.Fatalf(
					"%s %s status = %d, want %d; body=%s",
					test.Method,
					test.Path,
					response.Code,
					test.Status,
					response.Body,
				)
			}
		})
	}
	n.leaving = true
	response := httptest.NewRecorder()
	handler.ServeHTTP(
		response,
		httptest.NewRequest(http.MethodPost, "/issues", strings.NewReader(`{"title":"too late"}`)),
	)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("departing POST status = %d, want 503", response.Code)
	}
	view, err := readBoard(n.replica)
	if err != nil || len(view.Todo)+len(view.InProgress)+len(view.Done) != 0 {
		t.Fatalf("rejected requests changed the board: %+v, error=%v", view, err)
	}
}

func TestHTTPListenerAndCancellation(t *testing.T) {
	n := boardNode(t)
	n.config.HTTP = "127.0.0.1:0"
	var output bytes.Buffer
	n.output = &output
	ctx, cancel := context.WithCancel(t.Context())
	defer func() { cancel(); n.appWorkers.Wait() }()
	if err := n.startHTTP(ctx); err != nil {
		t.Fatal(err)
	}
	fields := strings.Fields(output.String())
	if len(fields) < 2 || strings.HasSuffix(fields[1], ":0") {
		t.Fatalf("bound HTTP address not printed: %q", output.String())
	}
	client := &http.Client{Timeout: time.Second}
	response, err := client.Get(fields[1] + "/board")
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.Copy(io.Discard, response.Body)
	if closeErr := response.Body.Close(); err != nil || closeErr != nil {
		t.Fatalf("read HTTP response: %v; close: %v", err, closeErr)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("GET /board status = %d", response.StatusCode)
	}
	cancel()
	n.appWorkers.Wait()
	response, err = client.Get(fields[1] + "/board")
	if err == nil {
		_ = response.Body.Close()
		t.Fatal("HTTP listener remained open after cancellation")
	}
}
