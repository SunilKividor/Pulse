package queues

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/SunilKividor/pkg/utils"
	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	connectionName = flag.String("connection-name", "jobs-producer", "the connection name to RabbitMQ")
)

type JobQueueConfig struct {
	User     string
	Password string
	AwsEc2IP string
	Port     string
}

type PublishingMessage struct {
	BucketName string `json:"bucket_name"`
	Key        string `json:"key"`
}

type PublishingMessageConfig struct {
	Message      *PublishingMessage
	ExchangeName *string
	ExchangeType *string
	QueueName    *string
	RoutingKey   *string
	Confirmation chan *amqp.DeferredConfirmation
}

func NewJobQueueConfig() *JobQueueConfig {
	return &JobQueueConfig{
		User:     os.Getenv("JOB_QUEUE_USER"),
		Password: os.Getenv("JOB_QUEUE_PASSWORD"),
		AwsEc2IP: os.Getenv("JOB_QUEUE_EC2_IP"),
		Port:     os.Getenv("JOB_QUEUE_PORT"),
	}
}

func NewPublishingMessage(bucketName string, key string) *PublishingMessage {
	return &PublishingMessage{
		BucketName: bucketName,
		Key:        key,
	}
}

func NewPublishingMessageConfig(message PublishingMessage, confirmationChan chan *amqp.DeferredConfirmation) *PublishingMessageConfig {
	exchangeName := os.Getenv("JOB_QUEUE_EXCHANGE")
	exchangeType := os.Getenv("JOB_QUEUE_EXCHANGE_TYPE")
	queueName := os.Getenv("JOB_QUEUE_NAME")
	routingKey := os.Getenv("JOB_QUEUE_ROUTING_KEY")
	return &PublishingMessageConfig{
		Message:      &message,
		ExchangeName: &exchangeName,
		ExchangeType: &exchangeType,
		QueueName:    &queueName,
		RoutingKey:   &routingKey,
		Confirmation: confirmationChan,
	}
}

func (jobQueue *JobQueueConfig) RunServer() *amqp.Connection {
	config := amqp.Config{
		Properties: amqp.NewConnectionProperties(),
	}
	config.Properties.SetClientConnectionName(*connectionName)
	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%s/", jobQueue.User, jobQueue.Password, jobQueue.AwsEc2IP, jobQueue.Port))
	utils.FailOnError(err, "Could not dial the job server")
	return conn
}

func (publishingMessageConfig *PublishingMessageConfig) PublishMessage(channel *amqp.Channel) error {
	msg, err := json.Marshal(publishingMessageConfig.Message)
	if err != nil {
		close(publishingMessageConfig.Confirmation)
		return utils.FailOnErrorWithoutPanic(err, "Error serializing Publishing-message")
	}

	err = channel.ExchangeDeclare(
		*publishingMessageConfig.ExchangeName,
		*publishingMessageConfig.ExchangeType,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		close(publishingMessageConfig.Confirmation)
		return utils.FailOnErrorWithoutPanic(err, "Error declaring Exchange")
	}

	queue, err := channel.QueueDeclare(
		*publishingMessageConfig.QueueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		close(publishingMessageConfig.Confirmation)
		return utils.FailOnErrorWithoutPanic(err, "Error declaring Queue")
	}

	err = channel.QueueBind(queue.Name, *publishingMessageConfig.RoutingKey, *publishingMessageConfig.ExchangeName, false, nil)
	if err != nil {
		close(publishingMessageConfig.Confirmation)
		return utils.FailOnErrorWithoutPanic(err, "Error binding queue")
	}

	dConfirmation, err := channel.PublishWithDeferredConfirm(*publishingMessageConfig.ExchangeName, *publishingMessageConfig.RoutingKey, true, false, amqp.Publishing{
		Body: []byte(msg),
	})
	if err != nil {
		close(publishingMessageConfig.Confirmation)
		return utils.FailOnErrorWithoutPanic(err, "Error publishing message")
	}

	publishingMessageConfig.Confirmation <- dConfirmation
	return nil
}
