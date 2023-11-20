package requests

import (
	proto "calculator/proto/gen"
	"context"
	"errors"
	"io"
	"log"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

func DoAvg(client proto.CalculatorServiceClient) {
	log.Printf("DoAvg invoked")

	stream, err := client.Avg(context.Background())
	if err != nil {
		log.Fatalf("Err while creating Avg stream: %+v", err)
	}

	reqs := []*proto.AvgRequest{
		{Number: 1},
		{Number: 2},
		{Number: 3},
		{Number: 4},
	}

	for _, req := range reqs {
		if err := stream.Send(req); err != nil {
			log.Fatalf("Err while sending Avg request: %+v", err)
		}
	}

	res, err := stream.CloseAndRecv()
	if err != nil {
		log.Fatalf("Err while receiving Avg result: %+v", err)
	}

	log.Printf("Avg: %.2f", res.Result)
}

func DoMax(client proto.CalculatorServiceClient) {
	log.Println("DoMax invoked")

	stream, err := client.Max(context.Background())
	if err != nil {
		log.Fatalf("Err while creating Max stream: %+v", err)
	}

	wait := make(chan struct{})

	go func() {
		numbers := []int64{4, 7, 2, 19, 4, 6, 32}

		for _, number := range numbers {
			stream.Send(&proto.MaxRequest{
				Number: number,
			})
		}

		stream.CloseSend()
	}()

	go func() {
		for {
			res, err := stream.Recv()
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}

				log.Printf("Failed to receive stream: %+v", err)
				break
			}

			log.Printf("Max: %d", res.Result)
		}

		close(wait)
	}()

	<-wait
}

func DoSqrt(client proto.CalculatorServiceClient) {
	log.Println("DoSqrt invoked")

	reqs := []*proto.SqrtRequest{
		{Number: 49},
		{Number: -49},
	}

	for _, req := range reqs {
		res, err := client.Sqrt(context.Background(), req)
		if err != nil {
			e, ok := status.FromError(err)
			if ok {
				log.Printf("Failed with server message: %s", e.Message())
				log.Printf("Failed with server code: %s", e.Code())

				if e.Code() == codes.InvalidArgument {
					log.Println("We probably sent a negative number")
					return
				}
			} else {
				log.Fatalf("Failed with a non gRPC Error: %+v", err)
			}
		}

		log.Printf("Sqrt: %.2f", res.Result)
	}

}
