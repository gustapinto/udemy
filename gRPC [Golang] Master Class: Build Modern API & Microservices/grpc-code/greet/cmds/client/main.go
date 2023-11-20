package main

import (
	proto "grpc-code/proto/gen"
	"log"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const SERVER_ADDR = "localhost:50051"

func main() {
	insecureOpts := grpc.WithTransportCredentials(insecure.NewCredentials())

	conn, err := grpc.Dial(SERVER_ADDR, insecureOpts)
	if err != nil {
		log.Fatalf("Failed to connect to gRPC srver at %s", SERVER_ADDR)
	}
	defer conn.Close()

	log.Printf("Connected to gRPC Server at %s", SERVER_ADDR)

	// Cria um novo cliente para o greet service
	client := proto.NewGreetServiceClient(conn)

	DoGreet(client)
	DoGreetManyTimes(client)
	DoLongGreet(client)
	DoGreetEveryone(client)
	DoGreetWithDeadline(client)
}
