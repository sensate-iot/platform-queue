package grpc

import (
	"context"
	"log"
	pb "platform-queue/proto"
)

type server struct {
	pb.UnimplementedMessageQueueServer
}

type MessageQueue struct {
	network string
	address string

	s server
	reflection bool
}

func New(network string, addr string, reflection bool) *MessageQueue {
	var mq = &MessageQueue{s: server{}, address: addr, network: network}

	mq.reflection = reflection
	return mq
}

func (s *server) EnqueueMessages(_ context.Context, in *pb.EnqueueRequest) (*pb.EnqueueResponse, error) {
	rv := &pb.EnqueueResponse{}

	rv.Message = "Messages queued"
	rv.Count = 1
	rv.ResponseId = generateResponseId()

	return rv, nil
}

func (s *server) DequeueMessages(_ context.Context, in *pb.DequeueRequest) (*pb.DequeueResponse, error) {
	rv := &pb.DequeueResponse{Count: 0}

	rv.Count = in.Count
	rv.ResponseId = generateResponseId()

	return rv, nil
}

func (mq *MessageQueue) Listen() {
	svc, serv := createGrpcServer(mq.network, mq.address, mq.reflection)

	pb.RegisterMessageQueueServer(serv, &mq.s)
	err := serv.Serve(svc)

	if err != nil {
		log.Fatal("GRPC service failure: ", err)
	}
}
