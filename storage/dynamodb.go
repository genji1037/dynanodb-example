package storage

import (
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type DB struct {
	svc *dynamodb.DynamoDB
}

func NewDB() *DB {
	svc := dynamodb.New(session.New())
	return &DB{svc: svc}
}
