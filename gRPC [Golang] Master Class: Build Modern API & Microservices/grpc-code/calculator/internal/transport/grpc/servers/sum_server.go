package servers

import (
	proto "calculator/proto/gen"
	"context"
	"log"
)

type CalculatorServer struct {
	proto.CalculatorServiceServer
}

func (s *CalculatorServer) Sum(ctx context.Context, req *proto.SumRequest) (res *proto.SumResponse, err error) {
	log.Printf("Sum invoked with: %+v", req)

	res = &proto.SumResponse{
		Result: req.X + req.Y,
	}

	return
}

func (s *CalculatorServer) Primes(req *proto.PrimesRequest, stream proto.CalculatorService_PrimesServer) (err error) {
	log.Printf("Primes invoked with: %+v", req)

	k := int64(2)
	n := req.Number

	for n > 1 {
		if n%k == 0 {
			stream.Send(&proto.PrimesResponse{
				Result: k,
			})

			n = n / k
			continue
		}

		k = k + 1
	}

	return
}
