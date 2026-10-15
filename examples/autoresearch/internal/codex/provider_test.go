package codex

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func testLogin(t *testing.T) (string, string) {
	t.Helper()
	claims := fmt.Sprintf(
		`{"exp":%d,"https://api.openai.com/auth":{"chatgpt_account_id":"account"}}`,
		time.Now().Add(time.Hour).Unix(),
	)
	token := "header." + base64.RawURLEncoding.EncodeToString([]byte(claims)) + ".signature"
	path := filepath.Join(t.TempDir(), "auth.json")
	data, _ := json.Marshal(map[string]any{"tokens": map[string]string{"access_token": token}})
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return path, token
}

func TestStatelessCodexToolContinuation(t *testing.T) {
	path, token := testLogin(t)
	before, _ := os.ReadFile(path)
	reasoning := json.RawMessage(`{"type":"reasoning","id":"reason","encrypted_content":"opaque"}`)
	function := json.RawMessage(
		`{"type":"function_call","call_id":"c1","name":"post","arguments":"{\"body\":\"finding\"}"}`,
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+token ||
			r.Header.Get("chatgpt-account-id") != "account" {
			t.Error("missing Codex authentication headers")
		}
		if r.Header.Get("session-id") != "run-agent-01" {
			t.Error("agent session IDs were not isolated")
		}
		var request struct {
			Model string            `json:"model"`
			Store bool              `json:"store"`
			Input []json.RawMessage `json:"input"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
		}
		if request.Model != "gpt-6-luna" || request.Store || len(request.Input) != 2 {
			t.Error("stateless model request changed")
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprintf(
			w,
			"data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":%s}\n\n",
			reasoning,
		)
		_, _ = fmt.Fprintf(
			w,
			"data: {\"type\":\"response.output_item.done\",\"output_index\":1,\"item\":%s}\n\n",
			function,
		)
		_, _ = io.WriteString(w, "data: {\"type\":\"response.completed\",\"response\":{}}\n\n")
	}))
	defer server.Close()
	client, err := New(path, "gpt-6-luna", "run")
	if err != nil {
		t.Fatal(err)
	}
	client.endpoint = server.URL
	result, err := client.Respond(context.Background(), Request{
		Actor: "agent-01", Input: []json.RawMessage{reasoning, function},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Items) != 2 || string(result.Items[0]) != string(reasoning) ||
		len(result.Calls) != 1 ||
		result.Calls[0].ID != "c1" {
		t.Fatalf("lost reasoning or tool call: %+v", result)
	}
	after, _ := os.ReadFile(path)
	if string(after) != string(before) {
		t.Fatal("external login was changed")
	}
}

func TestProviderRejectsIncompleteStreams(t *testing.T) {
	for _, input := range []string{
		"", "data: {\"type\":\"response.output_text.delta\",\"delta\":\"unfinished\"}\n\n",
		"data: {\"type\":\"response.incomplete\"}\n\n",
		"data: {\"type\":\"response.failed\"}\n\n", "data: {bad json}\n\n",
	} {
		if _, err := readResponse(strings.NewReader(input)); err == nil {
			t.Fatalf("accepted incomplete stream %q", input)
		}
	}
	if _, err := readResponse(strings.NewReader("")); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal(err)
	}
}

func TestCodexStreamWithoutContentType(t *testing.T) {
	path, _ := testLogin(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header()["Content-Type"] = nil
		_, _ = io.WriteString(
			w,
			"data: {\"type\":\"response.completed\",\"response\":{\"output\":[{\"type\":\"message\",\"content\":[{\"text\":\"ready\"}]}]}}\n\n",
		)
	}))
	defer server.Close()
	client, _ := New(path, "gpt-6-luna", "run")
	client.endpoint = server.URL
	result, err := client.Respond(context.Background(), Request{})
	if err != nil || result.Text != "ready" {
		t.Fatalf("headerless Codex stream: %q %v", result.Text, err)
	}
}

func TestSSEMultilineFinalEventAndLimits(t *testing.T) {
	input := ": heartbeat\n\ndata: {\"type\":\"response.completed\",\n" +
		"data: \"response\":{\"output\":[{\"type\":\"message\",\"content\":[{\"text\":\"hello\"}]}]}}"
	result, err := readResponse(strings.NewReader(input))
	if err != nil || result.Text != "hello" {
		t.Fatalf("multiline final event: %q %v", result.Text, err)
	}
	if _, err := readResponse(
		strings.NewReader("data: " + strings.Repeat("x", 1<<20)),
	); err == nil {
		t.Fatal("unbounded SSE event")
	}
}

func TestErrorsDoNotExposeLoginAndRedirectsAreNotFollowed(t *testing.T) {
	path, token := testLogin(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, token)
	}))
	defer server.Close()
	client, _ := New(path, "gpt-6-luna", "run")
	client.endpoint = server.URL
	if _, err := client.Respond(
		context.Background(),
		Request{},
	); err == nil ||
		strings.Contains(err.Error(), token) {
		t.Fatalf("credential masking failed: %v", err)
	}
	if err := validateEndpoint("https://example.com/responses"); err == nil {
		t.Fatal("accepted a remote override for subscription credentials")
	}
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, server.URL, http.StatusFound)
	}))
	defer redirect.Close()
	client.endpoint = redirect.URL
	if _, err := client.Respond(
		context.Background(),
		Request{},
	); err == nil ||
		!strings.Contains(err.Error(), "302") {
		t.Fatal("provider redirect was followed")
	}
}

func TestExpiredOrLoggedOutCredentials(t *testing.T) {
	for _, data := range []string{
		`{}`, `{bad`, `{"logged_out":true}`, `{"access_token":"not-a-token"}`,
		`{"access_token":"h.eyJleHAiOjF9.s","account_id":"account"}`,
	} {
		if _, err := decodeCredentials([]byte(data), time.Now()); err == nil {
			t.Fatalf("accepted %s", data)
		}
	}
}

func TestTransientStreamFailureRetriesBeforeExecutingTools(t *testing.T) {
	path, _ := testLogin(t)
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		if attempts.Add(1) == 1 {
			_, _ = io.WriteString(
				w,
				"data: {\"type\":\"error\",\"error\":{\"code\":\"server_is_overloaded\"}}\n\n",
			)
			return
		}
		_, _ = io.WriteString(
			w,
			"data: {\"type\":\"response.completed\",\"response\":{\"output\":[{\"type\":\"message\",\"content\":[{\"text\":\"recovered\"}]}]}}\n\n",
		)
	}))
	defer server.Close()
	client, _ := New(path, "gpt-6-luna", "run")
	client.endpoint = server.URL
	result, err := client.Respond(context.Background(), Request{})
	if err != nil || result.Text != "recovered" || attempts.Load() != 2 {
		t.Fatalf("retry: %q, %d attempts, %v", result.Text, attempts.Load(), err)
	}
}
