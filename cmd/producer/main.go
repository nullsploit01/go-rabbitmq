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

	if err := client.CreateQueue("customers_created", true, false); err != nil {
		panic(err)
	}

	if err := client.CreateQueue("customers_test", false, true); err != nil {
		panic(err)
	}

	if err := client.CreateBinding("customers_created", "customers.created.*", "customer_events"); err != nil {
		panic(err)
	}

	if err := client.CreateBinding("customers_test", "customers.*", "customer_events"); err != nil {
		panic(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Send(ctx, "customer_events", "customers.created.Persistent", amqp091.Publishing{
		ContentType:  "text/plain",
		DeliveryMode: amqp091.Persistent,
		Body:         []byte("Some Persistent test message"),
	}); err != nil {
		panic(err)
	}

	log.Println("Persistent Message sent")

	if err := client.Send(ctx, "customer_events", "customers.created.Transient", amqp091.Publishing{
		ContentType:  "text/plain",
		DeliveryMode: amqp091.Transient,
		Body:         []byte("Some Transient test message"),
	}); err != nil {
		panic(err)
	}

	log.Println("Transient Message sent")

	time.Sleep(100 * time.Second)
	log.Println(client)
}
