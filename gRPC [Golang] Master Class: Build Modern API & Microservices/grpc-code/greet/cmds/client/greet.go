package main

import (
	"context"
	"errors"
	proto "grpc-code/proto/gen"
	"io"
	"log"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

func DoLongGreet(client proto.GreetServiceClient) {
	log.Println("DoLongGreet invoked")

	reqs := []*proto.GreetRequest{
		{FirstName: "Foo"},
		{FirstName: "Bar"},
		{FirstName: "Foobar"},
	}
	stream, err := client.LongGreet(context.Background())
	if err != nil {
		log.Fatalf("Err while sending LongGreet: %+v", err)
	}

	for _, req := range reqs {
		if err := stream.Send(req); err != nil {
			log.Fatalf("Err while sending LongGreet request: %+v", err)
		}

		time.Sleep(1 * time.Second)
	}

	res, err := stream.CloseAndRecv()
	if err != nil {
		log.Fatalf("Err while processing responses for LongGreet: %+v", err)
	}

	log.Printf("LongGreet: %s", res.Result)
}

func DoGreetEveryone(client proto.GreetServiceClient) {
	log.Printf("DoGreetEveryone invoked")

	reqs := []*proto.GreetRequest{
		{FirstName: "Foo"},
		{FirstName: "Bar"},
		{FirstName: "Foobar"},
	}
	stream, err := client.GreetEveryone(context.Background())
	if err != nil {
		log.Fatalf("Err while creating GreetEveryone stream: %+v", err)
	}

	wait := make(chan struct{})

	go func() {
		for _, req := range reqs {
			if err := stream.Send(req); err != nil {
				log.Fatalf("Err while sending GreetEveryone request: %+v", err)
			}
		}

		if err := stream.CloseSend(); err != nil {
			log.Fatalf("Err while closing GreetEveryone stream: %+v", err)
		}
	}()

	go func() {
		for {
			res, err := stream.Recv()
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}

				log.Printf("Err while receiving message: %+v", err)
			}

			log.Printf("GreetEveryone: %s", res.Result)
		}

		close(wait)
	}()

	<-wait
}

func DoGreetWithDeadline(client proto.GreetServiceClient) {
	log.Println("DoGreetWithDeadlineCalled")

	timeouts := []int{5, 1}

	for _, timeout := range timeouts {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout))
		defer cancel()

		res, err := client.GreetWithDeadline(ctx, &proto.GreetRequest{
			FirstName: "Foo",
		})
		if err != nil {
			e, ok := status.FromError(err)
			if ok {
				if e.Code() == codes.DeadlineExceeded {
					log.Printf("Deadline exceeded")
					return
				}

				log.Fatalf("Unexpected gRPc error: %+v", e.Message())
			} else {
				log.Printf("Err while receive message wiht non gRPC error: %+v", err)
			}
		}

		log.Printf("GretWithDeadline: %s", res.Result)
	}
}
