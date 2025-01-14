package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"os/exec"

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
	trancodedVideosBucket := os.Getenv("TRANSCODED_VIDEOS_BUCKET")

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

		//download video from s3-1
		s3Client := s3.New(sess)
		key := event.Records[0].S3Events.Object.Key
		bucket := event.Records[0].S3Events.Bucket.Name
		err = downloadFromS3(s3Client, bucket, key)
		if err != nil {
			log.Printf("Error downloading from s3: %s", err.Error())
			continue
		}

		//ffmpeg video transcoding
		cmd := exec.Command("ffmpeg", "-i", key, "-b:v", "13000k", "13000-3.mp4")
		err = cmd.Run()
		if err != nil {
			log.Fatalf("%s", err.Error())
		}

		//delete file from s3-1
		decodedKey, err := url.QueryUnescape(key)
		if err != nil {
			log.Fatalf("error decording key: %s", err.Error())
		}
		deleteObjectInput := &s3.DeleteObjectInput{
			Bucket: &bucket,
			Key:    &decodedKey,
		}
		_, err = s3Client.DeleteObject(deleteObjectInput)
		if err != nil {
			log.Println("Could not delete the object from s3-2")
		}

		//upload file to s3-2
		file, err := os.Open("13000-3.mp4") //name from ffmpeg command
		if err != nil {
			log.Fatalf("could not open the transcoded file to upload")
		}
		putObjectInput := &s3.PutObjectInput{
			Bucket: &trancodedVideosBucket,
			Key:    aws.String("transcoded-video.mp4"),
			Body:   file,
		}
		_, err = s3Client.PutObject(putObjectInput)
		if err != nil {
			log.Println("error uploading file to bucket")
		}
	}
}

func downloadFromS3(s3Client *s3.S3, bucket, key string) error {
	file, err := os.Create(key)
	if err != nil {
		log.Printf("error creating file : %s", err.Error())
		return err
	}
	defer file.Close()

	decodedKey, err := url.QueryUnescape(key)
	if err != nil {
		log.Printf("error decording key: %s", err.Error())
		return err
	}

	getObjectInput := &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &decodedKey,
	}
	objectOutput, err := s3Client.GetObject(getObjectInput)
	if err != nil {
		log.Printf("could not download the object: %s", err.Error())
		return err
	}
	noOfBytes, err := io.Copy(file, objectOutput.Body)
	if err != nil {
		return err
	}
	fmt.Printf("no. of bytes copied: %v", noOfBytes)
	return nil
}
