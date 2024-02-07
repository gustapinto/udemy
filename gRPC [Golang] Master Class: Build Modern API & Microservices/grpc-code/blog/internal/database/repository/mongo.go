package repository

import (
	"blog/internal/config"
	"blog/internal/model"
	"context"
	"errors"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoRepository struct {
	collection *mongo.Collection
}

func NewMongoRepository() (repository *MongoRepository, err error) {
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(config.MONGO_DB_URI))
	if err != nil {
		return
	}

	database := client.Database(config.MONGO_DB_DATABASE_NAME)
	repository.collection = database.Collection(config.MONGO_DB_COLLECTION_NAME)
	return
}

func (m MongoRepository) CreateBlog(authorID, title, content string) (blogItem model.BlogItem, err error) {
	blogItem = model.BlogItem{
		AuthorID: authorID,
		Title:    title,
		Content:  content,
	}

	res, err := m.collection.InsertOne(context.Background(), blogItem)
	if err != nil {
		return
	}

	objectId, ok := res.InsertedID.(primitive.ObjectID)
	if !ok {
		err = errors.New("failed to convert ObjectID")
		return
	}

	blogItem.ID = objectId

	return
}
