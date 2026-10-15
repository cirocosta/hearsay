package hearsay_test

import (
	"context"
	"fmt"
	"time"

	"github.com/cirocosta/hearsay"
	"github.com/cirocosta/hearsay/link"
)

func exampleValue[T any](value T, err error) T {
	if err != nil {
		panic(err)
	}
	return value
}

func exampleCheck(err error) {
	if err != nil {
		panic(err)
	}
}

func ExampleRumors_GossipOnce() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	alice := exampleValue(exampleValue(hearsay.Seed(hearsay.BytesCodec())).IntoRumors())
	defer func() { _ = alice.Close() }()
	version := exampleValue(alice.Send([]byte("the meeting is at noon")))

	near, far := link.Memory()
	a, b := link.Manage(near), link.Manage(far)
	defer func() { _ = a.Close(); _ = b.Close() }()
	served := make(chan error, 1)
	serve := func() {
		go func() { _, err := alice.GossipOnce(ctx, a); served <- err }()
	}
	serve()
	joined := exampleValue(hearsay.Bootstrap(hearsay.BytesCodec()).Join(ctx, b))
	exampleCheck(<-served)
	bob := exampleValue(joined.Peer.IntoRumors())
	defer func() { _ = bob.Close() }()
	for message := range bob.Snapshot().All() {
		exampleCheck(message.Err)
		fmt.Println(string(message.Payload))
	}

	exampleCheck(alice.Redact(version))
	serve()
	_ = exampleValue(bob.GossipOnce(ctx, b))
	exampleCheck(<-served)
	fmt.Println("live messages:", bob.Snapshot().Len())
	// Output:
	// the meeting is at noon
	// live messages: 0
}
