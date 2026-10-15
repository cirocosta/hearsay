package codex

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
)

// readResponse retains completed output items, including opaque reasoning, for
// stateless continuation. A closed stream alone never means a completed turn.
func readResponse(reader io.Reader) (Turn, error) {
	bounded := &io.LimitedReader{R: reader, N: 16<<20 + 1}
	scanner := bufio.NewScanner(bounded)
	scanner.Buffer(make([]byte, 4096), 1<<20)
	stream := eventReader{scanner: scanner, bounded: bounded}
	state := streamState{items: make(map[int]json.RawMessage)}
	for {
		data, err := stream.next()
		if err != nil {
			return Turn{}, err
		}
		result, done, err := state.consume(data)
		if done || err != nil {
			return result, err
		}
	}
}

type eventReader struct {
	scanner *bufio.Scanner
	bounded *io.LimitedReader
	data    strings.Builder
}

func (s *eventReader) next() ([]byte, error) {
	for s.scanner.Scan() {
		line := s.scanner.Text()
		if line == "" && s.data.Len() != 0 {
			return s.event()
		}
		if err := s.append(line); err != nil {
			return nil, err
		}
	}
	if err := s.scanner.Err(); err != nil {
		if errors.Is(err, bufio.ErrTooLong) {
			return nil, err
		}
		return nil, transientError{err}
	}
	if s.data.Len() != 0 {
		return s.event()
	}
	return nil, transientError{io.ErrUnexpectedEOF}
}

func (s *eventReader) append(line string) error {
	if data, ok := strings.CutPrefix(line, "data:"); ok {
		s.data.WriteString(strings.TrimPrefix(data, " "))
		s.data.WriteByte('\n')
	}
	if s.data.Len() > 1<<20 || s.bounded.N == 0 {
		return errors.New("provider event or stream exceeds byte limit")
	}
	return nil
}

func (s *eventReader) event() ([]byte, error) {
	if s.bounded.N == 0 {
		return nil, errors.New("provider stream exceeds byte limit")
	}
	data := []byte(s.data.String())
	s.data.Reset()
	return data, nil
}

type streamState struct{ items map[int]json.RawMessage }

func (s *streamState) consume(data []byte) (Turn, bool, error) {
	var event struct {
		Type  string          `json:"type"`
		Index int             `json:"output_index"`
		Item  json.RawMessage `json:"item"`
		Error struct {
			Code string `json:"code"`
			Type string `json:"type"`
		} `json:"error"`
		Response struct {
			Output []json.RawMessage `json:"output"`
		} `json:"response"`
	}
	if err := json.Unmarshal(data, &event); err != nil {
		return Turn{}, false, err
	}
	switch event.Type {
	case "response.output_item.done":
		if event.Index < 0 || event.Index >= 256 || len(event.Item) == 0 {
			return Turn{}, false, errors.New("invalid output item index or body")
		}
		s.items[event.Index] = event.Item
		return Turn{}, false, nil
	case "response.completed":
		items := event.Response.Output
		if len(items) == 0 {
			items = s.completedItems()
		}
		result, err := completedTurn(items)
		return result, true, err
	case "response.failed", "response.incomplete", "error":
		err := fmt.Errorf("provider %s: %.1024s", event.Type, data)
		if event.Error.Code == "server_is_overloaded" ||
			event.Error.Code == "rate_limit_exceeded" ||
			event.Error.Type == "service_unavailable_error" {
			return Turn{}, true, transientError{err}
		}
		return Turn{}, true, err
	default:
		return Turn{}, false, nil
	}
}

func (s *streamState) completedItems() []json.RawMessage {
	var indices []int
	for index := range s.items {
		indices = append(indices, index)
	}
	sort.Ints(indices)
	items := make([]json.RawMessage, 0, len(indices))
	for _, index := range indices {
		items = append(items, s.items[index])
	}
	return items
}

func completedTurn(items []json.RawMessage) (Turn, error) {
	result := Turn{Items: items}
	for _, raw := range items {
		var item struct {
			Call
			Content []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
		}
		if err := json.Unmarshal(raw, &item); err != nil {
			return Turn{}, err
		}
		if item.Type == "function_call" {
			if !validCall(item.Call) {
				return Turn{}, errors.New("invalid completed function Call")
			}
			result.Calls = append(result.Calls, item.Call)
		}
		for _, part := range item.Content {
			result.Text += part.Text
		}
	}
	return result, nil
}

func validCall(c Call) bool {
	return c.ID != "" && c.Name != "" && json.Valid([]byte(c.Arguments))
}
