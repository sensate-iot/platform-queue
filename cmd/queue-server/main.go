package main

import (
	"log"
	"platform-queue/internal/grpc"
)

var (
	BuildVersion = "0.0.0"
)

func main() {
	log.Printf("Starting Sensate IoT Message Queue Server %s", BuildVersion)

	service := grpc.NewPlatformQueueServer("tcp", ":8080", false)
	service.Listen()
}
