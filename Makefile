#
#
#

GO := @go
GO_ENV := $(shell go env GOPATH)
PROTOC := @protoc

ifeq ($(OS),Windows_NT)
	QSERVER_EXEC = bin/queue-server.exe
else
	QSERVER_EXEC = bin/queue-server
endif

_all: deps all

all: build-rpc
	@echo Building queue-server
	$(GO) build -o $(QSERVER_EXEC) -ldflags "-s -w" platform-queue/cmd/queue-server

deps:
	@echo Downloading dependencies
	$(GO) mod download

build-grpc:
	$(PROTOC) --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		proto/message.proto proto/dequeuerequest.proto \
		 proto/enqueuerequest.proto proto/messagequeue.proto

test: deps
	@echo Running unit tests
	$(GO) test -cover ./...

get-protoc:
	$(GO) get google.golang.org/protobuf/cmd/protoc-gen-go
	$(GO) get google.golang.org/grpc/cmd/protoc-gen-go-grpc
