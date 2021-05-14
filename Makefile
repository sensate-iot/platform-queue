#
#
#

GO := @go
GO_ENV := $(shell go env GOPATH)
PROTOC := @protoc

ifeq ($(OS),Windows_NT)
	QSERVER_EXEC = bin/queue-server.exe
	QUEUE_SYNC_TEST = bin/sync-queue-test.exe
	QUEUE_ASYNC_TEST = bin/async-queue-test.exe
else
	QSERVER_EXEC = bin/queue-server
endif

_all: deps all

all: build-grpc
	@echo Building queue-server
	$(GO) build -o $(QSERVER_EXEC) -ldflags "-s -w" platform-queue/cmd/queue-server

build-tests: deps
	@echo Building synchronous queue benchmark
	$(GO) build -o $(QUEUE_SYNC_TEST)  platform-queue/test/queue/sync

	@echo Building asynchronous queue benchmark
	$(GO) build -o $(QUEUE_ASYNC_TEST)  platform-queue/test/queue/async

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
