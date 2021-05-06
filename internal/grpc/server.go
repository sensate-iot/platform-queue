package grpc

import (
	"github.com/google/uuid"
	"google.golang.org/grpc"
	refl "google.golang.org/grpc/reflection"
	"log"
	"net"
)

type Server interface {
	Listen()
}

func createGrpcServer(network string, address string, reflection bool) (net.Listener, *grpc.Server) {
	svc, err := net.Listen(network, address)

	if err != nil {
		log.Fatal("Unable to start server: ", err)
	}

	serv := grpc.NewServer()

	if reflection {
		refl.Register(serv)
	}

	return svc, serv
}

func generateResponseId() []byte {
	id := uuid.New()
	bytes, err := id.MarshalBinary()

	if err != nil {
		log.Fatal("Unable to create response ID.")
	}

	return bytes
}
