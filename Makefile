#
#
#

GO := @go
GO_ENV := $(shell go env GOPATH)
PROTOC := @protoc

_all: all

all: build-rpc
	@echo Building queue-server

build-rpc:
	$(PROTOC) --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		proto/message.proto proto/queuerequest.proto

get-protoc:
	$(GO) get google.golang.org/protobuf/cmd/protoc-gen-go
	$(GO) get google.golang.org/grpc/cmd/protoc-gen-go-grpc
