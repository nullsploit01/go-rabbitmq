package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/nullsploit01/go-rabbitmq/internal"
	"github.com/rabbitmq/amqp091-go"
)

func main() {
	publishConnection, err := internal.ConnectRabbitMQ("hardy", "password", "localhost:5671", "customers",
		"/home/nullsploit/projects/portfolio/go-rabbitmq/tls-gen/basic/result/ca_certificate.pem",
		"/home/nullsploit/projects/portfolio/go-rabbitmq/tls-gen/basic/result/client_poseidon_certificate.pem",
		"/home/nullsploit/projects/portfolio/go-rabbitmq/tls-gen/basic/result/client_poseidon_key.pem")
	if err != nil {
		panic(err)
	}

	consumeConnection, err := internal.ConnectRabbitMQ("hardy", "password", "localhost:5671", "customers",
		"/home/nullsploit/projects/portfolio/go-rabbitmq/tls-gen/basic/result/ca_certificate.pem",
		"/home/nullsploit/projects/portfolio/go-rabbitmq/tls-gen/basic/result/client_poseidon_certificate.pem",
		"/home/nullsploit/projects/portfolio/go-rabbitmq/tls-gen/basic/result/client_poseidon_key.pem")
	if err != nil {
		panic(err)
	}

	defer publishConnection.Close()
	defer consumeConnection.Close()

	publishClient, err := internal.NewRabbitMQClient(publishConnection)
	if err != nil {
		panic(err)
	}

	consumeClient, err := internal.NewRabbitMQClient(consumeConnection)
	if err != nil {
		panic(err)
	}

	defer publishClient.Close()
	defer consumeClient.Close()

	queue, err := consumeClient.CreateQueue("", true, true)
	if err != nil {
		panic(err)
	}

	if err := consumeClient.CreateBinding(queue.Name, queue.Name, "customer_callbacks"); err != nil {
		panic(err)
	}

	consumerMessageBus, err := consumeClient.Consume(queue.Name, "customer-api", true)

	if err != nil {
		panic(err)
	}

	go func() {
		for message := range consumerMessageBus {
			log.Printf("Callback msg: %s\n", message.Body)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for i := 0; i < 10; i++ {
		if err := publishClient.Send(ctx, "customer_events", "customers.created.Persistent", amqp091.Publishing{
			ContentType:   "text/plain",
			DeliveryMode:  amqp091.Persistent,
			Body:          []byte("Some Persistent test message"),
			ReplyTo:       queue.Name,
			CorrelationId: fmt.Sprintf("customer_created_%d", i),
		}); err != nil {
			panic(err)
		}

		log.Println("Persistent Message sent")
	}

	var blocker chan struct{}
	<-blocker
}
