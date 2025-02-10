package main

import (
	"encoding/json"
	"log"

	"github.com/SunilKividor/internal/config"
	"github.com/SunilKividor/internal/models"
	"github.com/SunilKividor/internal/queues"
	"github.com/SunilKividor/pkg/utils"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	amqp "github.com/rabbitmq/amqp091-go"
)

func init() {
	if err := godotenv.Load("../../configs/.env"); err != nil {
		utils.FailOnError(err, "Could not load .env")
	}
}

func main() {
	awsConfigModel := config.NewAwsConfigModel()
	awsConfig := awsConfigModel.NewAwsConfig()
	sess := session.Must(session.NewSessionWithOptions(
		session.Options{
			Config: awsConfig,
		},
	))

	//job queue- RABBITMQ
	jobQueueConfig := queues.NewJobRabbitMQConfig()
	con := jobQueueConfig.RunServer()
	ch, err := con.Channel()
	utils.FailOnError(err, "Channel connection")
	defer con.Close()
	defer ch.Close()

	//creating Queue in Job-Queue
	q := queues.NewJobQueue(ch, "transcoding-jobs")

	//sqs
	sqsClient := sqs.New(sess)
	sqsClientConfig := queues.NewSQSClientConfig()
	receiveMessageInput := sqsClientConfig.NewSQSReceiveMessageInput()

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
		var event models.S3Event
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
		msg := models.PublishingMessage{
			BucketName: event.Records[0].S3Events.Bucket.Name,
			Key:        event.Records[0].S3Events.Object.Key,
		}
		msgJson, err := json.Marshal(msg)
		utils.FailOnError(err, "Error serializing Publishing-message")
		err = ch.Publish("", q.Name, false, false, amqp.Publishing{
			Body: []byte(msgJson),
		})
		utils.FailOnError(err, "Error Publishing message")
	}
}
