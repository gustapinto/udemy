package main

import (
	"calculator/internal/config"
	"calculator/internal/transport/grpc/servers"
	proto "calculator/proto/gen"
	"log"
	"net"

	"google.golang.org/grpc"
)

func main() {
	listener, err := net.Listen("tcp", config.SERVER_ADDR)
	if err != nil {
		log.Fatalf("Failed to open listener: %+v", err)
	}
	defer func() {
		if err := listener.Close(); err != nil {
			log.Fatalf("Failed to close listener: %+v", err)
		}
	}()

	log.Printf("TCP server listening to %s", config.SERVER_ADDR)

	grpcServer := grpc.NewServer()

	proto.RegisterCalculatorServiceServer(grpcServer, &servers.Calculator{})

	log.Println("Registered CalculatorService")

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to attach gRPC server: %+v", err)
	}
}
