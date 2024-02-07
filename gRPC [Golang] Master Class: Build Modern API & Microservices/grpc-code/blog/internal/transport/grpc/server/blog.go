package server

import (
	database "blog/internal/database/repository"
	proto "blog/proto/gen"
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Blog struct {
	Repository *database.MongoRepository
}

func (b Blog) CreateBlog(ctx context.Context, req *proto.Blog) (blogID *proto.BlogId, err error) {
	blogItem, err := b.Repository.CreateBlog(req.AuthorId, req.Title, req.Content)
	if err != nil {
		err = status.Errorf(codes.Internal, "Failed to insert blog on database, got error: %+v", err)
		return
	}

	blogID = blogItem.ToProtoBlogID()
	return
}
