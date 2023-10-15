package main

import (
	"context"
	"fmt"
	proto "grpc-code/proto/gen"
	"log"
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
