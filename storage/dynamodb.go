package storage

import (
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

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
		Profile:           "dynamodb",
		Config:            cfg,
	}))

	svc := dynamodb.New(sess)
	return &DB{svc: svc}
}
