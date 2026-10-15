package main

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
)

type logger struct {
	mu     sync.Mutex
	output io.Writer
	events io.Writer
}

func (l *logger) print(actor, kind, text string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	text = strings.Join(strings.Fields(text), " ")
	if len(text) > 200 {
		text = text[:200] + "..."
	}
	_, _ = fmt.Fprintf(
		l.output,
		"%s %-8s %-9s %s\n",
		time.Now().Format("15:04:05"),
		actor,
		kind,
		text,
	)
}

func (l *logger) message(n note) {
	l.mu.Lock()
	_ = json.NewEncoder(l.events).Encode(n)
	l.mu.Unlock()
	text := fmt.Sprintf("%s %s %s %s", n.ID, n.Topic, n.Ref, n.Body)
	if n.Kind == "vote" {
		text = fmt.Sprintf("%s ref=%s agree=%t %s", n.ID, n.Ref, n.Agree, n.Body)
	}
	l.print(n.Author, n.Kind, text)
}
