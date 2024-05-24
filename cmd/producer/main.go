package main

import (
	"context"
	"log"
	"time"

	"github.com/nullsploit01/go-rabbitmq/internal"
	"github.com/rabbitmq/amqp091-go"
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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for i := 0; i < 10; i++ {
		if err := client.Send(ctx, "customer_events", "customers.created.Persistent", amqp091.Publishing{
			ContentType:  "text/plain",
			DeliveryMode: amqp091.Persistent,
			Body:         []byte("Some Persistent test message"),
		}); err != nil {
			panic(err)
		}

		log.Println("Persistent Message sent")
	}
}
