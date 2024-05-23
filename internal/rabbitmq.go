package internal

import (
	"fmt"

	"github.com/rabbitmq/amqp091-go"
)

type RabbitClient struct {
	conn    *amqp091.Connection
	channel *amqp091.Channel
}

func ConnectRabbitMQ(username, password, host, vhost string) (*amqp091.Connection, error) {
	return amqp091.Dial(fmt.Sprintf("amqp://%s:%s@%s/%s", username, password, host, vhost))
}

func NewRabbitMQClient(conn *amqp091.Connection) (RabbitClient, error) {
	ch, err := conn.Channel()

	if err != nil {
		return RabbitClient{}, err
	}

	return RabbitClient{
		conn:    conn,
		channel: ch,
	}, nil
}

func (rc RabbitClient) Close() error {
	return rc.channel.Close()
}

func (rc RabbitClient) CreateQueue(queueName string, durable, autoDelete bool) error {
	_, err := rc.channel.QueueDeclare(queueName, durable, autoDelete, false, false, nil)

	return err
}
