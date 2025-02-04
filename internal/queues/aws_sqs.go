package queues

import (
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type SQSClientConfig struct {
	QueryUrl string
}

func NewSQSClientConfig() *SQSClientConfig {
	return &SQSClientConfig{
		QueryUrl: os.Getenv("SQS_QUERYURL"),
	}
}

func (sqsClientConfig *SQSClientConfig) NewSQSReceiveMessageInput() sqs.ReceiveMessageInput {
	return sqs.ReceiveMessageInput{
		QueueUrl:            &sqsClientConfig.QueryUrl,
		VisibilityTimeout:   aws.Int64(30),
		WaitTimeSeconds:     aws.Int64(20),
		MaxNumberOfMessages: aws.Int64(1),
	}
}
