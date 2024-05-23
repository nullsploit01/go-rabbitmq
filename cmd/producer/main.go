package main

import (
	"log"
	"time"

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

	time.Sleep(10 * time.Second)
	log.Println(client)
}
