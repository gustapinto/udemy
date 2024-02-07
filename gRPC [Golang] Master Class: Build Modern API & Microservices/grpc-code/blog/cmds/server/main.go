package main

import (
	"blog/internal/config"
	"blog/internal/database/repository"
	"blog/internal/transport/grpc/server"
	proto "blog/proto/gen"
	"log"
	"net"

	"google.golang.org/grpc"
)

func main() {
	repository, err := repository.NewMongoRepository()
	if err != nil {
		log.Fatalf("Failed to connect to database, got error: %+v", err)
	}

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

	proto.RegisterBlogServiceServer(grpcServer, &server.Blog{
		Repository: &repository,
	})

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to attach gRPC server: %+v", err)
	}
}
