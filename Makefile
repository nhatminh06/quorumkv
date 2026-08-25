.PHONY: build test race vet check demo clean

build:
	mkdir -p bin
	go build -o bin/quorumkv ./cmd/quorumkv
	go build -o bin/qkv ./cmd/qkv

test:
	go test ./...

race:
	go test -race ./...

vet:
	go vet ./...

check: vet build test

demo: build
	./scripts/demo-basic.sh

clean:
	rm -rf bin
	rm -rf .local/quorumkv
