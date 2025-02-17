package main

import (
	"context"
	"encoding/json"
	"fmt"
	_ "github.com/joho/godotenv/autoload"
	"github.com/piveredu/piveredu-lib-messaging-v1/messaging"
	"log"
	"time"
)

var (
	client = messaging.New(context.TODO())
)

func init() {
	client.EstablishConnection()
}

func main() {
	event := messaging.NewEvent("piveredu.user", "piveredu.user.created", "piveredu.user.create")
	event.Metadata = map[string]any{
		"triggered_by": "bb4ef24b-1699-4452-ad09-f284e57c6049",
	}

	event.Medium = []string{"email", "sms", "push"}

	payload := map[string]any{
		"tenant": map[string]any{
			"id":          "bb4ef24b-1699-4452-ad09-f284e57c6049",
			"name":        "Prince of Peace",
			"description": "Prince of peace description",
			"createdAt":   time.Now().UnixMilli(),
		},
	}

	jb, err := json.Marshal(payload)
	if err != nil {
		log.Fatalln("failed to marshal payload", err)
	}

	event.Payload = jb

	jb, err = json.Marshal(event)
	if err != nil {
		log.Fatalln("failed to marshal event:", err)
	}

	if _, err := client.Publish("piveredu.user", jb, &messaging.PublishOptions{
		Args:        nil,
		AutoDelete:  false,
		ContentType: "application/json",
		Durable:     false,
		Exclusive:   false,
		Exchange:    "",
		Mandatory:   false,
		NoWait:      false,
		Immediately: false,
	}); err != nil {
		log.Fatalln("failed to send message: |", err)
	}

	messageEvents, err := client.Consume("piveredu.user", &messaging.ConsumeOptions{
		AutoAck:      false,
		ConsumerName: "",
		QueueName:    "piveredu.user",
		Durable:      false,
		Exclusive:    false,
		NoLocal:      false,
		NoWait:       false,
	})
	if err != nil {
		log.Println("failed to consume messages :::::: |", err)
	}

	for message := range messageEvents {
		log.Println("[ 💨 ] Received message:", message)
		log.Println("[ 💨 ] Received message payload:", string(message.Payload))

		if err := message.Acknowledger.Ack(message.Tag, false); err != nil {
			log.Fatalln(fmt.Errorf("failed to acknowledge message: %s", err))
		}
	}
}
