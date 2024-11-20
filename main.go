package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

//poll images to SQS
//then spin up docker containers

//sqs-user
//access-key : AKIAW3MEFI3VVZNI6K6U
//secret-access-key : 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

func main() {
	queryUrl := "https://sqs.us-east-1.amazonaws.com/471112959723/TempRawVideosS3Queue"
	cfg, err := config.LoadDefaultConfig(
		context.TODO(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("AKIAW3MEFI3VVZNI6K6U", "R7TComRi/nOVDn7/XjaeGdRy02VP6W8b2LPR3uey", ""),
		),
	)
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}
	sqsClient := sqs.NewFromConfig(cfg)

	for {
		output, err := sqsClient.ReceiveMessage(
			context.Background(),
			&sqs.ReceiveMessageInput{
				QueueUrl:            &queryUrl,
				MaxNumberOfMessages: 1,
				WaitTimeSeconds:     20,
			},
		)
		if err != nil {
			log.Printf("unable to get messages, %v", err)
			continue
		}
		if !(len(output.Messages) > 0) {
			log.Println("No messages")
			continue
		}
		//validate the msg
		msg1 := output.Messages[0]
		var event map[string]interface{}
		err = json.Unmarshal([]byte(*msg1.Body), &event)
		if err != nil {
			log.Println("error marshalling message body")
			continue
		}
		fmt.Println(event)

		//spin up the container

		//delete msg from queue
	}
}
