package internal

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"os"

	"github.com/rabbitmq/amqp091-go"
)

type RabbitClient struct {
	conn    *amqp091.Connection
	channel *amqp091.Channel
}

func ConnectRabbitMQ(username, password, host, vhost, caCert, clientCert, clientKey string) (*amqp091.Connection, error) {

	ca, err := os.ReadFile(caCert)
	if err != nil {
		return nil, err
	}

	cert, err := tls.LoadX509KeyPair(clientCert, clientKey)
	if err != nil {
		return nil, err
	}

	rootCAs := x509.NewCertPool()
	rootCAs.AppendCertsFromPEM(ca)

	tlsConfig := &tls.Config{
		RootCAs:      rootCAs,
		Certificates: []tls.Certificate{cert},
	}

	return amqp091.DialTLS(fmt.Sprintf("amqps://%s:%s@%s/%s", username, password, host, vhost), tlsConfig)
}

func NewRabbitMQClient(conn *amqp091.Connection) (RabbitClient, error) {
	ch, err := conn.Channel()

	if err != nil {
		return RabbitClient{}, err
	}

	if err := ch.Confirm(false); err != nil {
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

func (rc RabbitClient) CreateQueue(queueName string, durable, autoDelete bool) (amqp091.Queue, error) {
	queue, err := rc.channel.QueueDeclare(queueName, durable, autoDelete, false, false, nil)

	if err != nil {
		return amqp091.Queue{}, err
	}

	return queue, nil
}

func (rc RabbitClient) CreateBinding(name, binding, exchange string) error {
	return rc.channel.QueueBind(name, binding, exchange, false, nil)
}

func (rc RabbitClient) Send(ctx context.Context, exchange, routingKey string, options amqp091.Publishing) error {
	conf, err := rc.channel.PublishWithDeferredConfirmWithContext(ctx, exchange, routingKey, true, false, options)

	if err != nil {
		return err
	}

	log.Println(conf.Wait())
	return nil
}

func (rc RabbitClient) Consume(queue, consumer string, autoAck bool) (<-chan amqp091.Delivery, error) {
	return rc.channel.Consume(queue, consumer, autoAck, false, false, false, nil)
}

func (rc RabbitClient) ApplyQos(count, size int, global bool) error {
	return rc.channel.Qos(count, size, global)
}
