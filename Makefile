.PHONY: test race vet lint fmt check bench interop

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

interop:
	./interop/run.sh
