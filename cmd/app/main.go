package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/SunilKividor/internal/config"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/joho/godotenv"
	amqp "github.com/rabbitmq/amqp091-go"
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

type PublishingMessage struct {
	BucketName string `json:"bucket_name"`
	Key        string `json:"key"`
}

func init() {
	dir, _ := os.Getwd()
	fmt.Println(dir)
	if err := godotenv.Load("../configs/.env"); err != nil {
		log.Fatalf("Could not load .env")
	}
}

func main() {
	sqsQueryUrl := os.Getenv("SQS_QUERYURL")

	jobQueueUser := os.Getenv("JOB_QUEUE_USER")
	jobQueuePassword := os.Getenv("JOB_QUEUE_PASSWORD")
	jobQueueEc2IP := os.Getenv("JOB_QUEUE_EC2_IP")
	jobQueuePORT := os.Getenv("JOB_QUEUE_PORT")

	awsConfig, err := config.LoadAwsConfig()
	if err != nil {
		log.Fatal(err.Error())
	}
	awsSession := awsConfig.NewAwsSession()
	if err != nil {
		log.Fatal(awsSession)
	}

	//session
	sess := session.Must(session.NewSessionWithOptions(
		session.Options{
			Config: awsSession,
		},
	))

	//job queue- RABBITMQ
	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%s/", jobQueueUser, jobQueuePassword, jobQueueEc2IP, jobQueuePORT))
	failOnError(err, "Error could not connec to the job")
	defer conn.Close()
	ch, err := conn.Channel()
	failOnError(err, "Channel connection")
	defer ch.Close()

	//sqs
	sqsClient := sqs.New(sess)
	receiveMessageInput := sqs.ReceiveMessageInput{
		QueueUrl:            &sqsQueryUrl,
		VisibilityTimeout:   aws.Int64(30),
		WaitTimeSeconds:     aws.Int64(20),
		MaxNumberOfMessages: aws.Int64(1),
	}

	//creating Queue in Job-Queue
	q, err := ch.QueueDeclare("transcoding-jobs", false, false, false, false, nil)
	failOnError(err, "could not declare a transcoding-jobs queue")

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

		//publishing job to Job-Queue
		msg := PublishingMessage{
			BucketName: event.Records[0].S3Events.Bucket.Name,
			Key:        event.Records[0].S3Events.Object.Key,
		}
		msgJson, err := json.Marshal(msg)
		failOnError(err, "Error serializing Publishing-message")
		err = ch.Publish("", q.Name, false, false, amqp.Publishing{
			Body: []byte(msgJson),
		})
		failOnError(err, "Error Publishing message")
	}
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s,%s", msg, err.Error())
	}
}
