package main

import (
	"context"
	"errors"
	"fmt"
	proto "grpc-code/proto/gen"
	"io"
	"log"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) Greet(ctx context.Context, req *proto.GreetRequest) (*proto.GreetResponse, error) {
	log.Printf("Greet invoked with: %+v", req)

	return &proto.GreetResponse{
		Result: fmt.Sprintf("Hello %s", req.FirstName),
	}, nil
}

func (s *Server) GreetManyTimes(req *proto.GreetRequest, stream proto.GreetService_GreetManyTimesServer) (err error) {
	log.Printf("GreetManyTimes invoked with: %+v", req)

	for i := 1; i <= 10; i++ {
		stream.Send(&proto.GreetResponse{
			Result: fmt.Sprintf("Hello %s, number %d", req.FirstName, i),
		})
	}

	return
}

func (s *Server) LongGreet(stream proto.GreetService_LongGreetServer) error {
	log.Println("LongGreet invoked")

	result := ""

	for {
		req, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return stream.SendAndClose(&proto.GreetResponse{
					Result: result,
				})
			}

			log.Fatalf("Err while reading LongGreet %+v", err)
		}

		result += fmt.Sprintf("Hello %s! ", req.FirstName)
	}
}

func (s *Server) GreetEveryone(stream proto.GreetService_GreetEveryoneServer) error {
	log.Printf("GreetEveryone invoked")

	for {
		req, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}

			log.Fatalf("Err while reading GreetEveryone: %+v", err)
		}

		err = stream.Send(&proto.GreetResponse{
			Result: fmt.Sprintf("Hello %s!\n", req.FirstName),
		})
		if err != nil {
			log.Fatalf("Err while sending response: %+v", err)
		}
	}
}

func (Server) GreetWithDeadline(ctx context.Context, req *proto.GreetRequest) (res *proto.GreetResponse, err error) {
	log.Printf("GreetWithDeadline invoked with %+v", req)

	for i := 0; i < 3; i++ {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			log.Println("Client deadline exceeded")

			err = status.Error(codes.Canceled, "Client deadline exceeded")
		}

		time.Sleep(1 * time.Second)
	}

	res = &proto.GreetResponse{
		Result: fmt.Sprintf("Hello %s!", req.FirstName),
	}
	return
}
