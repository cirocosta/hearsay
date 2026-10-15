package codex

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Tool describes one function exposed by the example.
type Tool struct {
	Type        string         `json:"type"`
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Parameters  map[string]any `json:"parameters"`
	Strict      bool           `json:"strict"`
}

// Request contains a stateless continuation, including opaque prior items.
type Request struct {
	Actor        string
	Instructions string
	Input        []json.RawMessage
	Tools        []Tool
}

const codexURL = "https://chatgpt.com/backend-api/codex/responses"

// Call is a completed provider function call.
type Call struct {
	Type      string `json:"type"`
	ID        string `json:"call_id"`
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

// Turn retains completed output for the next request and exposes tool calls.
type Turn struct {
	Items []json.RawMessage
	Calls []Call
	Text  string
}

// Client keeps gx's stateless Codex request, authentication, and SSE pattern.
type Client struct {
	client   *http.Client
	endpoint string
	authFile string
	model    string
	runID    string
}

type transientError struct{ error }

func (e transientError) Unwrap() error { return e.error }

// Respond performs one authenticated streaming request.
func (r *Client) Respond(ctx context.Context, input Request) (Turn, error) {
	var result Turn
	var err error
	for attempt := range 4 {
		result, err = r.respondOnce(ctx, input)
		var transient transientError
		if !errors.As(err, &transient) || attempt == 3 {
			return result, err
		}
		if err := retryPause(ctx, time.Duration(1<<attempt)*time.Second); err != nil {
			return Turn{}, err
		}
	}
	return result, err
}

func retryPause(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay + time.Duration(rand.IntN(250))*time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (r *Client) respondOnce(ctx context.Context, input Request) (Turn, error) {
	credentials, err := readCredentials(r.authFile)
	if err != nil {
		return Turn{}, err
	}
	result, err := r.request(ctx, input, credentials)
	if err != nil {
		masked := errors.New(strings.ReplaceAll(err.Error(), credentials.Token, "[redacted]"))
		var transient transientError
		if errors.As(err, &transient) {
			return Turn{}, transientError{masked}
		}
		return Turn{}, masked
	}
	return result, nil
}

func (r *Client) request(
	ctx context.Context,
	input Request,
	auth credentials,
) (Turn, error) {
	if err := validateEndpoint(r.endpoint); err != nil {
		return Turn{}, err
	}
	body, err := json.Marshal(map[string]any{
		"model": r.model, "instructions": input.Instructions, "input": input.Input,
		"tools": input.Tools, "store": false, "stream": true,
		"parallel_tool_calls": true, "include": []string{"reasoning.encrypted_content"},
		"reasoning": map[string]string{"effort": "medium"},
	})
	if err != nil {
		return Turn{}, err
	}
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		r.endpoint,
		bytes.NewReader(body),
	)
	if err != nil {
		return Turn{}, err
	}
	request.Header = codexHeaders(auth, r.runID+"-"+input.Actor)
	client := *r.client
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	response, err := client.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return Turn{}, ctx.Err()
		}
		return Turn{}, transientError{err}
	}
	defer func() { _ = response.Body.Close() }()
	return decodeResponse(response)
}

func decodeResponse(response *http.Response) (Turn, error) {
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		err := fmt.Errorf("codex HTTP %d: %s", response.StatusCode, body)
		if response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500 {
			return Turn{}, transientError{err}
		}
		return Turn{}, err
	}
	contentType := response.Header.Get("Content-Type")
	if contentType != "" && !strings.HasPrefix(contentType, "text/event-stream") {
		return Turn{}, fmt.Errorf(
			"codex response is not an event stream (content type %q)",
			contentType,
		)
	}
	return readResponse(response.Body)
}

func codexHeaders(auth credentials, session string) http.Header {
	h := make(http.Header)
	h.Set("Content-Type", "application/json")
	h.Set("Accept", "text/event-stream")
	h.Set("Authorization", "Bearer "+auth.Token)
	h.Set("chatgpt-account-id", auth.Account)
	h.Set("OpenAI-Beta", "Client=experimental")
	h.Set("originator", "hearsay")
	h.Set("User-Agent", "hearsay-autoresearch")
	h.Set("session-id", session)
	return h
}

func validateEndpoint(endpoint string) error {
	u, err := url.Parse(endpoint)
	if err != nil {
		return err
	}
	if endpoint == codexURL {
		return nil
	}
	loopback := u.Hostname() == "127.0.0.1" || u.Hostname() == "localhost" || u.Hostname() == "::1"
	if !loopback || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil ||
		u.Fragment != "" {
		return errors.New("codex endpoint override must be loopback")
	}
	return nil
}

// New checks a read-only login and constructs a client for the Codex endpoint.
func New(authFile, modelName, runID string) (*Client, error) {
	if _, err := readCredentials(authFile); err != nil {
		return nil, err
	}
	return &Client{
		client: &http.Client{Timeout: 2 * time.Minute}, endpoint: codexURL,
		authFile: authFile, model: modelName, runID: runID,
	}, nil
}
