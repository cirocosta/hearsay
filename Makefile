.PHONY: test race vet lint fmt check bench

test:
	go test ./...

race:
	go test -race ./...

vet:
	go vet ./...

lint:
	golangci-lint run ./...

fmt:
	golangci-lint fmt ./...

check: test race vet lint

bench:
	go test -run='^$$' -bench=. -benchmem ./...
