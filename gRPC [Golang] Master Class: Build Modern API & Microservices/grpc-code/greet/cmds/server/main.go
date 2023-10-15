package main

import (
	"log"
	"net"

	pb "grpc-code/proto/gen"

	"google.golang.org/grpc"
)

const ADDR = "0.0.0.0:50051"

type Server struct {
	pb.GreetServiceServer
}

func main() {
	// Inicia um novo listener TCP
	listener, err := net.Listen("tcp", ADDR)
	if err != nil {
		log.Fatalf("Err while opening listener: %+v\n", err)
	}
	defer listener.Close()

	log.Printf("Server listening on: %s", ADDR)

	// Cria um server gRPC
	server := grpc.NewServer()

	// Aponta o servidor gRPC para o listener TCP aberto
	if err := server.Serve(listener); err != nil {
		log.Fatalf("Failed to start gRPC server: %+v\n", err)
	}
}
