package main

import (
	"context"
	"errors"
	proto "grpc-code/proto/gen"
	"io"
	"log"
)

func DoGreet(client proto.GreetServiceClient) {
	log.Println("DoGreet invoked")

	res, err := client.Greet(context.Background(), &proto.GreetRequest{
		FirstName: "Foo",
	})
	if err != nil {
		log.Fatalf("Err while processing Greet: %+v", err)
	}

	log.Printf("Greeting: %s", res.Result)
}

func DoGreetManyTimes(client proto.GreetServiceClient) {
	log.Println("DoGreetManyTimes invoked")

	req := &proto.GreetRequest{
		FirstName: "bar",
	}

	stream, err := client.GreetManyTimes(context.Background(), req)
	if err != nil {
		log.Fatalf("Err while sending GreetManyTimes: %+v", err)
	}

	for {
		msg, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			log.Fatalf("Err while procssing message from GreetManyTimes stream: %+v", err)
		}

		log.Printf("GreetManyTimes: %s", msg.Result)
	}
}
