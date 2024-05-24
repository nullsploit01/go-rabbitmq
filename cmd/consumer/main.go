package main

import (
	"context"
	"log"
	"time"

	"github.com/nullsploit01/go-rabbitmq/internal"
	"golang.org/x/sync/errgroup"
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

	queue, err := client.CreateQueue("", true, false)

	if err != nil {
		panic(err)
	}

	if err := client.CreateBinding(queue.Name, "", "customer_events"); err != nil {
		panic(err)
	}

	messageBus, err := client.Consume(queue.Name, "email-service", false)
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

				log.Printf("Acknowledged message: %s\n", msg.Body)

				return nil
			})
		}
	}()

	<-blocker
}
