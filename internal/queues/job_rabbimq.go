package queues

import (
	"fmt"
	"os"

	"github.com/SunilKividor/pkg/utils"
	amqp "github.com/rabbitmq/amqp091-go"
)

type JobRabbitMQConfig struct {
	User     string
	Password string
	AwsEc2IP string
	Port     string
}

func NewJobRabbitMQConfig() *JobRabbitMQConfig {
	return &JobRabbitMQConfig{
		User:     os.Getenv("JOB_QUEUE_USER"),
		Password: os.Getenv("JOB_QUEUE_PASSWORD"),
		AwsEc2IP: os.Getenv("JOB_QUEUE_EC2_IP"),
		Port:     os.Getenv("JOB_QUEUE_PORT"),
	}
}

func (jobQueue *JobRabbitMQConfig) RunServer() *amqp.Connection {
	con, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%s/", jobQueue.User, jobQueue.Password, jobQueue.AwsEc2IP, jobQueue.Port))
	utils.FailOnError(err, "Could not dial the job server")
	return con
}

func NewJobQueue(ch *amqp.Channel, name string) amqp.Queue {
	queue, err := ch.QueueDeclare(name, false, false, false, false, nil)
	utils.FailOnError(err, "Could not dial the job server")
	return queue
}
