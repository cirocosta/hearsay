package routed

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"testing"

	"github.com/cirocosta/hearsay/link"
)

// the same fixture compiles against the pre-pooling baseline, whose Config
// has no DisablePooling field and always opens fresh connections.
func benchmarkPoolConfig(disabled bool) Config {
	config := Config{}
	flag := reflect.ValueOf(&config).Elem().FieldByName("DisablePooling")
	if flag.IsValid() {
		flag.SetBool(disabled)
	}
	return config
}

func benchmarkEndpoint(b *testing.B, config Config) (*Endpoint, []byte) {
	b.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	address, err := EncodeTCPAddr(listener.Addr().(*net.TCPAddr).AddrPort())
	if err != nil {
		b.Fatal(err)
	}
	endpoint, err := New(listener, address, TCPDial, config)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := endpoint.Close(); err != nil {
			b.Error(err)
		}
	})
	return endpoint, address
}

func benchmarkLinks(b *testing.B, config Config) (*Link, *Link) {
	b.Helper()
	near, _ := benchmarkEndpoint(b, config)
	far, address := benchmarkEndpoint(b, config)
	_, left, err := near.Link(b.Context(), address)
	if err != nil {
		b.Fatal(err)
	}
	_, right, err := far.Accept(b.Context())
	if err != nil {
		b.Fatal(err)
	}
	return left, right
}

func benchmarkRemote(ctx context.Context, remote *Link, buffer []byte, streams, rounds int) error {
	var epilogue [1]byte
	for range rounds {
		for range streams {
			reader, err := remote.Accept(ctx)
			if err != nil {
				return err
			}
			if _, err = io.ReadFull(reader, buffer); err != nil {
				_ = reader.Close()
				return err
			}
			if err = link.Complete(reader); err != nil {
				return err
			}
		}
		if _, err := io.ReadFull(remote.Control(), epilogue[:]); err != nil {
			return err
		}
		if err := writeRouting(remote.Control(), epilogue[:]); err != nil {
			return err
		}
	}
	return nil
}

func benchmarkLocal(ctx context.Context, local *Link, payload []byte, streams int) error {
	for range streams {
		writer, err := local.Open(ctx)
		if err != nil {
			return err
		}
		if err = writeRouting(writer, payload); err != nil {
			_ = writer.Close()
			return err
		}
		if err = link.Complete(writer); err != nil {
			return err
		}
	}
	if err := writeRouting(local.Control(), []byte{1}); err != nil {
		return err
	}
	var epilogue [1]byte
	_, err := io.ReadFull(local.Control(), epilogue[:])
	return err
}

func BenchmarkRoutedSessions(b *testing.B) {
	for _, streams := range []int{1, 17} {
		for _, size := range []int{128, 4096} {
			for _, disabled := range []bool{false, true} {
				mode := "pooled"
				if disabled {
					mode = "fresh"
				}
				b.Run(fmt.Sprintf("streams%d/bytes%d/%s", streams, size, mode), func(b *testing.B) {
					local, remote := benchmarkLinks(b, benchmarkPoolConfig(disabled))
					payload := make([]byte, size)
					buffer := make([]byte, size)
					for i := range payload {
						payload[i] = byte(i)
					}
					done := make(chan error, 1)
					go func() { done <- benchmarkRemote(b.Context(), remote, buffer, streams, b.N) }()
					b.ReportAllocs()
					b.SetBytes(int64(streams * size))
					b.ResetTimer()
					for range b.N {
						if err := benchmarkLocal(b.Context(), local, payload, streams); err != nil {
							b.Fatal(err)
						}
					}
					b.StopTimer()
					if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
						b.Fatal(err)
					}
				})
			}
		}
	}
}
