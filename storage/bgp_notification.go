package storage

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/genji1037/dynanodb-example/alg"
	"github.com/genji1037/dynanodb-example/progress"
	"github.com/genji1037/dynanodb-example/tool"
	"github.com/gocql/gocql"
	"time"
)

const (
	BGPNotificationTableName = "bgpnotifications"
	BatchWriteMaxItemNumber  = 25
)

type BGPNotification struct {
	CnvID   string
	NID     string
	Payload string
}

func (db *DB) CreateBGPNotificationTable() {
	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("conv"),
				AttributeType: aws.String("S"),
			},
			{
				AttributeName: aws.String("id"),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("conv"),
				KeyType:       aws.String("HASH"),
			},
			{
				AttributeName: aws.String("id"),
				KeyType:       aws.String("RANGE"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
		TableName: aws.String(BGPNotificationTableName),
	}

	result, err := db.svc.CreateTable(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case dynamodb.ErrCodeResourceInUseException:
				fmt.Println(dynamodb.ErrCodeResourceInUseException, aerr.Error())
			case dynamodb.ErrCodeLimitExceededException:
				fmt.Println(dynamodb.ErrCodeLimitExceededException, aerr.Error())
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

	fmt.Println(result)
}

func (db *DB) AlterBGPNotification() {
	input := &dynamodb.UpdateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("conv"),
				AttributeType: aws.String("S"),
			},
			{
				AttributeName: aws.String("id"),
				AttributeType: aws.String("S"),
			},
		},
		TableName: aws.String(BGPNotificationTableName),
	}

	result, err := db.svc.UpdateTable(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case dynamodb.ErrCodeResourceInUseException:
				fmt.Println(dynamodb.ErrCodeResourceInUseException, aerr.Error())
			case dynamodb.ErrCodeResourceNotFoundException:
				fmt.Println(dynamodb.ErrCodeResourceNotFoundException, aerr.Error())
			case dynamodb.ErrCodeLimitExceededException:
				fmt.Println(dynamodb.ErrCodeLimitExceededException, aerr.Error())
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

	fmt.Println(result)
}

func (db *DB) DescribeBGPNotificationTable() {
	input := &dynamodb.DescribeTableInput{
		TableName: aws.String(BGPNotificationTableName),
	}

	result, err := db.svc.DescribeTable(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case dynamodb.ErrCodeResourceNotFoundException:
				fmt.Println(dynamodb.ErrCodeResourceNotFoundException, aerr.Error())
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

	fmt.Println(result)
}

func (db *DB) QueryBGPNotificationsByCnvID(cnvID, nid string, limit int64) string {
	u := &gocql.UUID{}
	err := u.UnmarshalText([]byte(nid))
	if err != nil {
		return err.Error()
	}
	nid = alg.ToSortableTimeUUID(*u)
	return db.queryBGPNotificationsByCnvID(cnvID, nid, limit)
}

func (db *DB) queryBGPNotificationsByCnvID(cnvID, nid string, limit int64) string {
	input := &dynamodb.QueryInput{
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":cnv_id": {
				S: aws.String(cnvID),
			},
			":nid": {
				S: aws.String(nid),
			},
		},
		KeyConditionExpression: aws.String("conv = :cnv_id AND id > :nid"),
		ProjectionExpression:   aws.String("id, payload"),
		TableName:              aws.String(BGPNotificationTableName),
		Limit:                  &limit,
		ReturnConsumedCapacity: aws.String("TOTAL"),
	}
	startAt := time.Now()
	result, err := db.svc.Query(input)

	cost := time.Now().Sub(startAt)
	progress.ObserveRead(cost)
	fmt.Println(cost, progress.P.ReadCount)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case dynamodb.ErrCodeProvisionedThroughputExceededException:
				fmt.Println(dynamodb.ErrCodeProvisionedThroughputExceededException, aerr.Error())
			case dynamodb.ErrCodeResourceNotFoundException:
				fmt.Println(dynamodb.ErrCodeResourceNotFoundException, aerr.Error())
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
		return ""
	}
	var readSize int
	for _, item := range result.Items {
		readSize += alg.RunCount(*item["payload"].S, *item["id"].S)
	}
	fmt.Println("read size:", readSize)
	return result.String()
}

func (db *DB) PutBGPNotification(notification BGPNotification) {
	input := &dynamodb.PutItemInput{
		Item: map[string]*dynamodb.AttributeValue{
			"conv": {
				S: aws.String(notification.CnvID),
			},
			"id": {
				S: aws.String(alg.ToSortableTimeUUID(gocql.TimeUUID())),
			},
			"payload": {
				S: aws.String(notification.Payload),
			},
		},
		ReturnConsumedCapacity: aws.String("TOTAL"),
		TableName:              aws.String(BGPNotificationTableName),
	}

	fmt.Println("write size:", alg.RunCount(notification.CnvID, alg.ToSortableTimeUUID(gocql.TimeUUID()), notification.Payload))

	startAt := time.Now()
	output, err := db.svc.PutItem(input)
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
	tool.PrintFloatIfNotNil("rcu:", output.ConsumedCapacity.ReadCapacityUnits)
	tool.PrintFloatIfNotNil("wcu:", output.ConsumedCapacity.WriteCapacityUnits)
	tool.PrintFloatIfNotNil("cu:", output.ConsumedCapacity.CapacityUnits)
}

func (db *DB) BatchWriteBGPNotification(notifications []BGPNotification) {
	var requestItems map[string][]*dynamodb.WriteRequest
	var writeRequests []*dynamodb.WriteRequest

	writeBatch := func() {
		input := &dynamodb.BatchWriteItemInput{
			RequestItems: requestItems,
		}
		result, err := db.svc.BatchWriteItem(input)
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok {
				switch aerr.Code() {
				case dynamodb.ErrCodeProvisionedThroughputExceededException:
					fmt.Println(dynamodb.ErrCodeProvisionedThroughputExceededException, aerr.Error())
				case dynamodb.ErrCodeResourceNotFoundException:
					fmt.Println(dynamodb.ErrCodeResourceNotFoundException, aerr.Error())
				case dynamodb.ErrCodeItemCollectionSizeLimitExceededException:
					fmt.Println(dynamodb.ErrCodeItemCollectionSizeLimitExceededException, aerr.Error())
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
		fmt.Println(result)
	}

	for i := range notifications {
		batchIndex := i % BatchWriteMaxItemNumber
		if batchIndex == 0 {
			requestItems = make(map[string][]*dynamodb.WriteRequest)
			requestLength := len(notifications) - i
			if requestLength > BatchWriteMaxItemNumber {
				requestLength = BatchWriteMaxItemNumber
			}
			writeRequests = make([]*dynamodb.WriteRequest, requestLength)
			requestItems[BGPNotificationTableName] = writeRequests
		}
		writeRequests[batchIndex] = &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: map[string]*dynamodb.AttributeValue{
					"conv": {
						S: aws.String(notifications[i].CnvID),
					},
					"id": {
						S: aws.String(alg.ToSortableTimeUUID(gocql.TimeUUID())),
					},
					"payload": {
						S: aws.String(notifications[i].Payload),
					},
				},
			},
		}
		if batchIndex == BatchWriteMaxItemNumber-1 {
			writeBatch()
		}
	}

	if len(notifications)%BatchWriteMaxItemNumber != 0 {
		writeBatch()
	}
}

func (db *DB) DelBGPNotification() {
	input := &dynamodb.DeleteTableInput{
		TableName: aws.String(BGPNotificationTableName),
	}

	result, err := db.svc.DeleteTable(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case dynamodb.ErrCodeResourceInUseException:
				fmt.Println(dynamodb.ErrCodeResourceInUseException, aerr.Error())
			case dynamodb.ErrCodeResourceNotFoundException:
				fmt.Println(dynamodb.ErrCodeResourceNotFoundException, aerr.Error())
			case dynamodb.ErrCodeLimitExceededException:
				fmt.Println(dynamodb.ErrCodeLimitExceededException, aerr.Error())
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

	fmt.Println(result)
}
