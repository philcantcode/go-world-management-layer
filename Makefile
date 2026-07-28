BUF_VERSION := v1.50.0
PROTOC_GEN_GO_VERSION := v1.36.5
PROTOC_GEN_GO_GRPC_VERSION := v1.5.1

.PHONY: generate generate-tools verify module-check schema-check fuzz-seeds contract-check security-check integration-check test test-race vet linux-check fmt-check

generate-tools:
	go install github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION)
	go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)

generate:
	buf generate api/world/v1/world.proto

verify:
	go run ./cmd/verify

module-check:
	go run ./cmd/verify -only=module

schema-check:
	go run ./cmd/verify -only=schema

fuzz-seeds:
	go run ./cmd/verify -only=fuzz

contract-check:
	go run ./cmd/verify -only=contract

security-check:
	go run ./cmd/verify -only=security

integration-check:
	go run ./cmd/verify -only=integration

test:
	go test ./...

test-race:
	go test -race ./...

vet:
	go vet ./...

linux-check:
	go run ./cmd/verify -only=linux

fmt-check:
	go run ./cmd/verify -only=format
