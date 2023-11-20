package main

import (
	"calculator/internal/config"
	"calculator/internal/transport/grpc/requests"
	proto "calculator/proto/gen"
	"log"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	transportCredentials := grpc.WithTransportCredentials(insecure.NewCredentials())
	conn, err := grpc.Dial(config.SERVER_ADDR, transportCredentials)
	if err != nil {
		log.Fatalf("Failed to connect to gRPC server: %+v", err)
	}
	defer conn.Close()

	log.Printf("Connected to gRPC server at %s", config.SERVER_ADDR)

	client := proto.NewCalculatorServiceClient(conn)

	requests.DoSum(client)
	requests.DoPrimes(client)
	requests.DoAvg(client)
	requests.DoMax(client)
	requests.DoSqrt(client)
}
