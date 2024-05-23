package main

import (
	"log"

	"github.com/nullsploit01/go-rabbitmq/internal"
)

func main() {
	conn, err := internal.ConnectRabbitMQ("hardy", "password", "localhost:5672", "customers")

	if err != nil {
		panic(err)
	}

	defer conn.Close()

	client, err := internal.NewRabbitMQClient(conn)

	if err != nil {
		panic(err)
	}

	defer client.Close()

	messageBus, err := client.Consume("customers_created", "email-service", false)
	if err != nil {
		panic(err)
	}

	var blocking chan struct{}

	go func() {
		for message := range messageBus {
			log.Printf("New Message: %v\n", message)

			if err := message.Ack(false); err != nil {
				log.Println("Message acknowledgement failed.")
				continue
			}

			log.Printf("Message %s Acknowledged\n", message.MessageId)
		}
	}()

	<-blocking
}
