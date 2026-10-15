package main

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"os"

	h "github.com/cirocosta/hearsay"
	"github.com/cirocosta/hearsay/before"
)

func runScenario(provider string) error {
	s, err := newScenario(provider)
	if err != nil {
		return err
	}
	defer s.close()
	steps := []struct {
		name string
		run  func() error
	}{
		{"bootstrap", s.bootstrap},
		{"two-way divergence", s.diverge},
		{"bidirectional 1-MiB payloads", s.largePayloads},
		{"bidirectional redactions", s.redact},
		{"Rust-Go-Rust transitive deletion", s.transitive},
		{"270 repeated sessions/epoch wrap", s.repeat},
		{"mutual retirement declined and link reused", s.mutualRetire},
		{"retire, survivor send, rebootstrap", s.retireAndRejoin},
	}
	for _, step := range steps {
		if err := step.run(); err != nil {
			return fmt.Errorf("%s: %w", step.name, err)
		}
		if err := s.check(); err != nil {
			return fmt.Errorf("%s: %w", step.name, err)
		}
		fmt.Printf("%s-provider %s passed\n", provider, step.name)
	}
	return nil
}

func (s *scenario) bootstrap() error {
	if s.provider == "rust" {
		if os.Getenv("HEARSAY_INTEROP_EMPTY") != "1" {
			if err := s.sendRust([]byte("rust before bootstrap")); err != nil {
				return err
			}
		}
		return s.rust.pair("gossip", s.join)
	}
	var err error
	s.rumors, err = seed()
	if err != nil {
		return err
	}
	if os.Getenv("HEARSAY_INTEROP_EMPTY") != "1" {
		if _, err := s.rumors.Send([]byte("go before bootstrap")); err != nil {
			return err
		}
	}
	return s.rust.pair("bootstrap", s.gossip)
}

func (s *scenario) sendRust(payload []byte) error {
	_, err := s.rust.command(map[string]any{"op": "send", "payload": hex.EncodeToString(payload)})
	return err
}

func (s *scenario) diverge() error {
	for i := 0; i < 40; i++ {
		version, err := s.rumors.Send([]byte(fmt.Sprintf("go concurrent %d", i)))
		if err != nil {
			return err
		}
		if i == 0 {
			s.victim = version
		}
		if err := s.sendRust([]byte(fmt.Sprintf("rust concurrent %d", i))); err != nil {
			return err
		}
	}
	return s.sync()
}

func (s *scenario) largePayloads() error {
	if _, err := s.rumors.Send(bytes.Repeat([]byte{0xa5}, 1<<20)); err != nil {
		return err
	}
	if err := s.sendRust(bytes.Repeat([]byte{0x5a}, 1<<20)); err != nil {
		return err
	}
	return s.sync()
}

func (s *scenario) redact() error {
	victim, err := findRustPayload(s.rust, "rust concurrent 0")
	if err != nil {
		return err
	}
	if err := s.rumors.Redact(victim); err != nil {
		return err
	}
	if _, err := s.rust.command(
		map[string]any{"op": "redact", "version": hexVersion(s.victim)},
	); err != nil {
		return err
	}
	return s.sync()
}

func findRustPayload(r *rust, payload string) (h.Version, error) {
	state, err := r.op("list")
	if err != nil {
		return h.Version{}, err
	}
	expected := hex.EncodeToString([]byte(payload))
	for _, record := range state.Records {
		if record.Payload != expected {
			continue
		}
		encoded, err := hex.DecodeString(record.Version)
		if err != nil {
			return h.Version{}, err
		}
		return before.DecodeVersion(encoded)
	}
	return h.Version{}, fmt.Errorf("no message payload %q", payload)
}

func (s *scenario) transitive() error {
	second, err := startRust("bootstrap")
	if err != nil {
		return err
	}
	defer second.close()
	relay := &scenario{
		ctx:      s.ctx,
		rust:     second,
		endpoint: s.endpoint,
		provider: "rust",
		rumors:   s.rumors,
	}
	if err := relay.connect(); err != nil {
		return err
	}
	defer func() { _ = relay.carrier.Close() }()
	if err := second.pair("bootstrap", relay.gossip); err != nil {
		return err
	}
	if err := relay.check(); err != nil {
		return err
	}
	victim, err := findRustPayload(s.rust, "go concurrent 1")
	if err != nil {
		return err
	}
	if _, err := s.rust.command(
		map[string]any{"op": "redact", "version": hexVersion(victim)},
	); err != nil {
		return err
	}
	if err := s.sync(); err != nil {
		return err
	}
	if err := relay.sync(); err != nil {
		return err
	}
	if err := relay.check(); err != nil {
		return err
	}
	if s.rumors.Snapshot().Contains(victim) {
		return fmt.Errorf("deleted record survived relay")
	}
	return nil
}

func (s *scenario) repeat() error {
	initialOpened, initialDials := s.opened, s.dials.Load()
	var wrapped [3]bool
	for i := 0; i < 270; i++ {
		if err := s.repeatRound(i, &wrapped); err != nil {
			return fmt.Errorf("round %d: %w", i, err)
		}
	}
	if wrapped != [3]bool{true, true, true} {
		return fmt.Errorf("missing data-bearing wrapped epochs: %v", wrapped)
	}
	opened, dials := s.opened-initialOpened, s.dials.Load()-initialDials
	if opened <= dials {
		return fmt.Errorf(
			"no Go connection reuse: %d outgoing data streams, %d fresh dials",
			opened,
			dials,
		)
	}
	fmt.Printf(
		"%s-provider READY reuse: %d outgoing data streams, %d fresh dials\n",
		s.provider,
		opened,
		dials,
	)
	return nil
}

func (s *scenario) repeatRound(round int, wrapped *[3]bool) error {
	epoch := s.carrier.State().Epoch
	if err := s.repeatPayloads(round, epoch); err != nil {
		return err
	}
	before := s.opened
	if err := s.sync(); err != nil {
		return err
	}
	if epoch <= 2 {
		if s.opened == before {
			return fmt.Errorf("wrapped epoch %d opened no data streams", epoch)
		}
		wrapped[epoch] = true
	}
	return nil
}

func (s *scenario) repeatPayloads(round int, epoch uint8) error {
	if round%32 != 0 && epoch < 254 && epoch > 2 {
		return nil
	}
	if _, err := s.rumors.Send(
		[]byte(fmt.Sprintf("go reuse round %d epoch %d", round, epoch)),
	); err != nil {
		return err
	}
	return s.sendRust([]byte(fmt.Sprintf("rust reuse round %d epoch %d", round, epoch)))
}

func (s *scenario) retireGo(expect h.RetirementStatus) error {
	peer, err := s.rumors.TryIntoPeer()
	if err != nil {
		return err
	}
	result := peer.Retire(s.ctx, s.carrier)
	if result.Err != nil {
		return result.Err
	}
	if result.Status != expect {
		return fmt.Errorf("retirement status %v; want %v", result.Status, expect)
	}
	s.rumors = nil
	if result.Peer != nil {
		s.rumors, err = result.Peer.IntoRumors()
	}
	return err
}

func (s *scenario) mutualRetire() error {
	err := s.rust.retirement("declined", func() error { return s.retireGo(h.RetirementDeclined) })
	if err != nil {
		return err
	}
	return s.sync()
}

func (s *scenario) retireAndRejoin() error {
	if s.provider == "rust" {
		if err := s.rust.retirement("retired", s.gossip); err != nil {
			return err
		}
		if _, err := s.rumors.Send([]byte("after Rust retired")); err != nil {
			return err
		}
		return s.rust.pair("bootstrap", s.gossip)
	}
	if err := s.rust.pair("gossip", func() error { return s.retireGo(h.Retired) }); err != nil {
		return err
	}
	if err := s.sendRust([]byte("after Go retired")); err != nil {
		return err
	}
	return s.rust.pair("gossip", s.join)
}
