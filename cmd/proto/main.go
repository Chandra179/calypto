package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"

	userpb "gosdk/api/proto/gen/proto/user"
)

type UserServer struct {
	userpb.UnimplementedUserServiceServer
}

func (s *UserServer) CreateUser(ctx context.Context, req *userpb.CreateUserRequest) (*emptypb.Empty, error) {
	fmt.Println("📥  Server received CreateUser:", req.Name, req.Email)
	return &emptypb.Empty{}, nil
}

func (s *UserServer) GetUser(ctx context.Context, req *userpb.GetUserRequest) (*userpb.GetUserResponse, error) {
	fmt.Println("📥  Server received GetUser:", req.Id)
	return &userpb.GetUserResponse{
		User: &userpb.User{
			Id:    req.Id,
			Name:  "Test User",
			Email: "test@example.com",
		},
	}, nil
}

func main() {
	go startServer() // Start server in background

	// Wait a moment for server to boot
	// (in real production you don't do this)
	// but for simulation it's fine
	select {
	case <-context.Background().Done():
	case <-func() chan struct{} {
		ch := make(chan struct{})
		go func() {
			// tiny delay
			<-time.After(300 * time.Millisecond)
			close(ch)
		}()
		return ch
	}():
	}

	startClient()
}

func startServer() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatal("failed to listen:", err)
	}

	grpcServer := grpc.NewServer()
	userpb.RegisterUserServiceServer(grpcServer, &UserServer{})

	fmt.Println("🚀 gRPC server running on :50051")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatal("server failed:", err)
	}
}

func startClient() {
	conn, err := grpc.NewClient("localhost:50051",
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal("failed to connect:", err)
	}
	defer conn.Close()

	client := userpb.NewUserServiceClient(conn)

	// ------------------------
	// CREATE USER (Unary Call)
	// ------------------------
	fmt.Println("➡ Sending CreateUser...")
	_, err = client.CreateUser(context.Background(), &userpb.CreateUserRequest{
		Name:  "Chandra",
		Email: "chandra@example.com",
	})
	if err != nil {
		log.Fatal("CreateUser error:", err)
	}

	// ------------------------
	// GET USER (Unary Call)
	// ------------------------
	fmt.Println("➡ Sending GetUser...")
	res, err := client.GetUser(context.Background(), &userpb.GetUserRequest{
		Id: "123",
	})
	if err != nil {
		log.Fatal("GetUser error:", err)
	}

	fmt.Println("📦 GetUser Response:", res.User)
}
