package model

import (
	proto "blog/proto/gen"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type BlogItem struct {
	ID       primitive.ObjectID `bson:"_id, omitempty"`
	AuthorID string             `bson:"author_id"`
	Title    string             `bson:"title"`
	Content  string             `bson:"content"`
}

func (b BlogItem) ToProtoBlog() *proto.Blog {
	return &proto.Blog{
		Id:       b.ID.Hex(),
		AuthorId: b.AuthorID,
		Title:    b.Title,
		Content:  b.Content,
	}
}
