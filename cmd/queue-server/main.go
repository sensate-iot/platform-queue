package main

/*import (
	"context"
	"google.golang.org/grpc"
	"log"
	"net"
	pb "platform-queue/proto"
)

var (
	BuildVersion = "0.0.0"
)

type server struct {
	pb.UnimplementedGreeterServer
}

func (s *server) SayHello(ctx context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
	return &pb.HelloReply{Message: "Hi.." + in.GetName()}, nil
}

func main() {
	log.Printf("Starting Sensate IoT Queue Server %s", BuildVersion)

	svc, err := net.Listen("tcp", ":8080")

	if err != nil {
		log.Fatal("Unable to start server: ", err)
	}

	serv := grpc.NewServer()

	pb.RegisterGreeterServer(serv, &server{})
	err = serv.Serve(svc)

	if err != nil {
		log.Fatal("GRPC service failure: ", err)
	}
}*/

func main() {

}
