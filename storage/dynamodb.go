package storage

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type DB struct {
	svc *dynamodb.DynamoDB
}

func NewDB() *DB {
	t := true
	cfg := aws.Config{
		CredentialsChainVerboseErrors: &t,
	}

	sess, err := session.NewSession(&cfg)
	if err != nil {
		fmt.Println(err.Error())
		return nil
	}
	svc := dynamodb.New(sess)
	return &DB{svc: svc}
}
