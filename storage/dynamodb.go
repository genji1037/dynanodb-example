package storage

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type DB struct {
	svc *dynamodb.DynamoDB
}

func NewDB() *DB {
	sess, err := session.NewSession()
	if err != nil {
		fmt.Println(err.Error())
		return nil
	}
	svc := dynamodb.New(sess)
	return &DB{svc: svc}
}
