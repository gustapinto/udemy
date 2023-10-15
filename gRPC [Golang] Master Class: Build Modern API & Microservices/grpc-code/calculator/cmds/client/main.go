package main

import (
	"calculator/internal/config"
	proto "calculator/proto/gen"
	"context"
	"errors"
	"io"
	"log"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func DoSum(client proto.CalculatorServiceClient) {
	log.Println("DoSum invoked")

	res, err := client.Sum(context.Background(), &proto.SumRequest{
		X: 3,
		Y: 10,
	})
	if err != nil {
		log.Fatalf("Err while sending Sum: %+v", err)
	}

	log.Printf("Result: %d", res.Result)
}

func DoPrimes(client proto.CalculatorServiceClient) {
	log.Println("DoPrimes invoked")

	stream, err := client.Primes(context.Background(), &proto.PrimesRequest{
		Number: 120,
	})
	if err != nil {
		log.Fatalf("Err while sending Primes: %+v", err)
	}

	for {
		msg, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			log.Fatalf("Failed while processing message from Primes: %+v", err)
		}

		log.Printf("Primes: %d", msg.Result)
	}
}

func main() {
	transportCredentials := grpc.WithTransportCredentials(insecure.NewCredentials())
	conn, err := grpc.Dial(config.SERVER_ADDR, transportCredentials)
	if err != nil {
		log.Fatalf("Failed to connect to gRPC server: %+v", err)
	}
	defer conn.Close()

	log.Printf("Connected to gRPC server at %s", config.SERVER_ADDR)

	client := proto.NewCalculatorServiceClient(conn)

	DoSum(client)
	DoPrimes(client)
}
