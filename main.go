package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/joho/godotenv"
)

type S3Event struct {
	Records []Records `json:"Records"`
}

type Records struct {
	S3Events S3 `json:"s3"`
}

type S3 struct {
	SchemaVersion   string   `json:"s3SchemaVersion"`
	ConfigurationId string   `json:"configurationId"`
	Bucket          S3Bucket `json:"bucket"`
	Object          S3Object `json:"object"`
}

type S3Bucket struct {
	Name string `json:"name"`
	ARN  string `json:"arn"`
}

type S3Object struct {
	Key       string `json:"key"`
	Size      int    `json:"size"`
	ETag      string `json:"eTag"`
	Sequencer string `json:"sequencer"`
}

func init() {
	if err := godotenv.Load(); err != nil {
		log.Fatal("Could not load .env")
	}
}

func main() {
	awsAccessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	awsSecretAccessKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	awsSQSRegion := os.Getenv("AWS_SQS_REGION")
	sqsQueryUrl := os.Getenv("SQS_QUERYURL")
	//configs
	config := aws.Config{
		Credentials: credentials.NewStaticCredentials(
			awsAccessKey,
			awsSecretAccessKey,
			"",
		),
		Region: &awsSQSRegion,
	}
	//session
	sess := session.Must(session.NewSessionWithOptions(
		session.Options{
			Config: config,
		},
	))
	//sqs
	sqsClient := sqs.New(sess)
	receiveMessageInput := sqs.ReceiveMessageInput{
		QueueUrl:            &sqsQueryUrl,
		VisibilityTimeout:   aws.Int64(30),
		WaitTimeSeconds:     aws.Int64(20),
		MaxNumberOfMessages: aws.Int64(1),
	}
	//polling for messages
	for {
		result, err := sqsClient.ReceiveMessage(&receiveMessageInput)
		if err != nil {
			log.Printf("Error receiving message: %v", err)
			continue
		}
		if !(len(result.Messages) > 0) {
			log.Println("No new messages")
			continue
		}
		message := result.Messages[0]
		var event S3Event
		err = json.Unmarshal([]byte(*message.Body), &event)
		if err != nil {
			log.Println("error unmarshalling the json")
			continue
		}
		if !(len(event.Records) > 0) {
			log.Println("No Records")
			continue
		}
		fmt.Println(event.Records[0].S3Events.Object.Key)

		//download video from s3
		s3Client := s3.New(sess)
		key := event.Records[0].S3Events.Object.Key
		bucket := event.Records[0].S3Events.Bucket.Name
		err = downloadFromS3(s3Client, bucket, key)
		if err != nil {
			log.Println("Error downloading from s3")
		}
	}
}

func downloadFromS3(s3Client *s3.S3, bucket, key string) error {
	// fileName := filepath.Base(key)
	// filePath := filepath.Join("downloads", fileName)
	return nil
}
