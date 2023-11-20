package servers

import (
	proto "calculator/proto/gen"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Calculator struct {
	proto.CalculatorServiceServer
}

func (s *Calculator) Sum(ctx context.Context, req *proto.SumRequest) (res *proto.SumResponse, err error) {
	log.Printf("Sum invoked with: %+v", req)

	res = &proto.SumResponse{
		Result: req.X + req.Y,
	}

	return
}

func (s *Calculator) Primes(req *proto.PrimesRequest, stream proto.CalculatorService_PrimesServer) (err error) {
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

func (Calculator) calculateAvg(numbers []float64) float64 {
	log.Printf("%+v", numbers)
	total := 0.0

	for _, number := range numbers {
		total += number
	}

	return total / float64(len(numbers))
}

func (s Calculator) Avg(stream proto.CalculatorService_AvgServer) (err error) {
	log.Printf("Avg invoked with: %+v", stream)

	numbers := make([]float64, 0)

	for {
		req, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return stream.SendAndClose(&proto.AvgResponse{
					Result: s.calculateAvg(numbers),
				})
			}
		}

		numbers = append(numbers, float64(req.Number))
	}
}

func (Calculator) Max(stream proto.CalculatorService_MaxServer) (err error) {
	log.Printf("Max invoked with: %+v", stream)

	var maximum int64

	for {
		req, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}

			log.Fatalf("Error while reading request: %+v", err)
		}

		if req.Number > maximum {
			maximum = req.Number

			err := stream.Send(&proto.MaxResponse{
				Result: maximum,
			})
			if err != nil {
				log.Fatalf("Error while sending response, %+v", err)
			}
		}
	}
}

func (Calculator) Sqrt(ctx context.Context, req *proto.SqrtRequest) (res *proto.SqrtResponse, err error) {
	log.Printf("Sqrt invoked with %+v", req)

	if req.Number < 0 {
		err = status.Error(codes.InvalidArgument, fmt.Sprintf("Received negative number: %d", req.Number))
		return
	}

	res = &proto.SqrtResponse{
		Result: math.Sqrt(float64(req.Number)),
	}
	return
}
