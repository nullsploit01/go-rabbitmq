package main

import (
	"context"
	"log"
	"time"

	"github.com/nullsploit01/go-rabbitmq/internal"
	"github.com/rabbitmq/amqp091-go"
	"golang.org/x/sync/errgroup"
)

func main() {
	consumerConnection, err := internal.ConnectRabbitMQ("hardy", "password", "localhost:5672", "customers")
	if err != nil {
		panic(err)
	}
	defer consumerConnection.Close()

	publisherConnection, err := internal.ConnectRabbitMQ("hardy", "password", "localhost:5672", "customers")
	if err != nil {
		panic(err)
	}
	defer publisherConnection.Close()

	consumerClient, err := internal.NewRabbitMQClient(consumerConnection)
	if err != nil {
		panic(err)
	}
	defer consumerClient.Close()

	publisherClient, err := internal.NewRabbitMQClient(publisherConnection)
	if err != nil {
		panic(err)
	}
	defer publisherClient.Close()

	queue, err := consumerClient.CreateQueue("", true, false)

	if err != nil {
		panic(err)
	}

	if err := consumerClient.CreateBinding(queue.Name, "", "customer_events"); err != nil {
		panic(err)
	}

	messageBus, err := consumerClient.Consume(queue.Name, "email-service", false)
	if err != nil {
		panic(err)
	}

	var blocker chan struct{}

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	g, _ := errgroup.WithContext(ctx)

	g.SetLimit(10)

	go func() {
		for message := range messageBus {
			msg := message
			g.Go(func() error {
				// log.Printf("New message: %v", msg)

				if err := msg.Ack(false); err != nil {
					log.Println("Message acknowledgement failed")
					return err
				}

				if err := publisherClient.Send(ctx, "customer_callbacks", msg.ReplyTo, amqp091.Publishing{
					ContentType:   "plain/tetx",
					DeliveryMode:  amqp091.Persistent,
					Body:          []byte("RPC Callback message"),
					CorrelationId: msg.CorrelationId,
				}); err != nil {
					return err
				}

				log.Printf("Acknowledged message: %s\n", msg.Body)

				return nil
			})
		}
	}()

	<-blocker
}
