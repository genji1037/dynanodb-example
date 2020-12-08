package storage

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/genji1037/dynanodb-example/alg"
	"github.com/genji1037/dynanodb-example/progress"
	"github.com/gocql/gocql"
	"time"
)

const NotificationTableName = "notifications"

type Notification struct {
	UserID  string
	NID     string
	Payload string
}

func (db *DB) PutNotification(notification Notification) {
	input := &dynamodb.PutItemInput{
		Item: map[string]*dynamodb.AttributeValue{
			"user": {
				S: aws.String(notification.UserID),
			},
			"id": {
				S: aws.String(alg.ToSortableTimeUUID(gocql.TimeUUID())),
			},
			"payload": {
				S: aws.String(notification.Payload),
			},
		},
		TableName: aws.String(NotificationTableName),
	}

	startAt := time.Now()
	_, err := db.svc.PutItem(input)
	cost := time.Now().Sub(startAt)
	progress.ObserveWrite(cost)
	fmt.Println(cost, progress.P.WriteCount)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case dynamodb.ErrCodeConditionalCheckFailedException:
				fmt.Println(dynamodb.ErrCodeConditionalCheckFailedException, aerr.Error())
			case dynamodb.ErrCodeProvisionedThroughputExceededException:
				fmt.Println(dynamodb.ErrCodeProvisionedThroughputExceededException, aerr.Error())
			case dynamodb.ErrCodeResourceNotFoundException:
				fmt.Println(dynamodb.ErrCodeResourceNotFoundException, aerr.Error())
			case dynamodb.ErrCodeItemCollectionSizeLimitExceededException:
				fmt.Println(dynamodb.ErrCodeItemCollectionSizeLimitExceededException, aerr.Error())
			case dynamodb.ErrCodeTransactionConflictException:
				fmt.Println(dynamodb.ErrCodeTransactionConflictException, aerr.Error())
			case dynamodb.ErrCodeRequestLimitExceeded:
				fmt.Println(dynamodb.ErrCodeRequestLimitExceeded, aerr.Error())
			case dynamodb.ErrCodeInternalServerError:
				fmt.Println(dynamodb.ErrCodeInternalServerError, aerr.Error())
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}
		return
	}
}
