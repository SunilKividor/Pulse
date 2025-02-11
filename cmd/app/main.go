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
	"github.com/joho/godotenv"
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

	//job queue server - RABBITMQ
	jobQueueConfig := queues.NewJobQueueConfig()
	queueServerConn := jobQueueConfig.RunServer()
	queueConnChannel, err := queueServerConn.Channel()
	utils.FailOnError(err, "Channel connection")
	defer queueServerConn.Close()
	defer queueConnChannel.Close()

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
		doneCh := make(chan struct{})
		confirmChan := make(chan *amqp.DeferredConfirmation)
		go func() {
			for {
				select {
				case dConf, ok := <-confirmChan:
					if !ok {
						log.Println("Confirmation channel closed. Exiting goroutine...")
						return
					}
					log.Println("Received confirmation:", dConf)

				case <-doneCh:
					log.Println("Received stop signal. Exiting goroutine...")
					return
				}
			}
		}()
		msg := queues.NewPublishingMessage(event.Records[0].S3Events.Bucket.Name, event.Records[0].S3Events.Object.Key)
		publishingMsgCongig := queues.NewPublishingMessageConfig(
			*msg,
			confirmChan,
		)
		err = publishingMsgCongig.PublishMessage(queueConnChannel)
		if err == nil {
			close(doneCh)
		}
	}
}
