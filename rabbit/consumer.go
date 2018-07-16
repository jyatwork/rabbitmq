package rabbit

import (
	"fmt"

	"github.com/alecthomas/log4go"
	"github.com/streadway/amqp"
)

type Consumer struct {
	connection *amqp.Connection
	channel    *amqp.Channel
	tag        string
	done       chan error
}

func NewConsumer(conn *amqp.Connection, exchange, exchangeType, queueName, key, ctag string, f func(amqp.Delivery)) (*Consumer, error) {

	var err error
	c := &Consumer{
		connection: conn,
		channel:    nil,
		tag:        ctag,
		done:       make(chan error),
	}

	//log4go.Debug("got Connection, getting Channel")
	c.channel, err = c.connection.Channel()
	if err != nil {
		log4go.Error(err.Error())
		return nil, fmt.Errorf("RabbitMQ Channel: %s", err.Error())
	}
	//	if err := c.channel.Qos(1000, 0, true); err != nil {
	//		log4go.Error("c.channel.Qos:", err.Error())
	//	}
	//log4go.Debug("got Channel, declaring Exchange (%q)", exchange)
	if err := c.channel.ExchangeDeclare(
		exchange,     // name of the exchange
		exchangeType, // type
		true,         // durable
		false,        // delete when complete
		false,        // internal
		false,        // noWait
		nil,          // arguments
	); err != nil {
		return nil, fmt.Errorf("RabbitMQ Exchange Declare: %s", err.Error())
	}

	//log4go.Debug("declared Exchange, declaring Queue %q", queueName)
	queue, err := c.channel.QueueDeclare(
		queueName, // name of the queue
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // noWait
		nil,       // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("RabbitMQ Queue Declare: %s", err.Error())
	}

	//log4go.Debug("declared Queue (%q %d messages, %d consumers), binding to Exchange (key %q)",
	//	queue.Name, queue.Messages, queue.Consumers, key)
	if err := c.channel.QueueBind(
		queue.Name, // name of the queue
		key,        // bindingKey
		exchange,   // sourceExchange
		false,      // noWait
		nil,        // arguments
	); err != nil {
		return nil, fmt.Errorf("RabbitMQ Queue Bind: %s", err.Error())
	}

	//log4go.Debug("Queue bound to Exchange, starting Consume (consumer tag %q)", c.tag)
	deliveries, err := c.channel.Consume(
		queue.Name, // name
		c.tag,      // consumerTag,
		false,      // noAck
		false,      // exclusive
		false,      // noLocal
		false,      // noWait
		nil,        // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("RabbitMQ Queue Consume: %s", err)
	}

	go handle(deliveries, c.done, f)

	return c, nil
}

var dataChan chan (amqp.Delivery)

//noinspection ALL
func handle(deliveries <-chan amqp.Delivery, done chan error, f func(amqp.Delivery)) {
	dataChan = make(chan (amqp.Delivery), 100)
	//	for d := range deliveries {
	//		f(d)
	//	}
	go func() {
		for {

			dataChan <- <-deliveries

		}
	}()
	go func() {
		for {

			select {
			case d := <-dataChan:

				f(d)
			}
		}
	}()

	//log4go.Error("RabbitMQ handle: deliveries channel closed")
	//done <- nil
}

func (c *Consumer) Shutdown() error {
	// close the deliveries channel
	if err := c.channel.Cancel(c.tag, true); err != nil {
		log4go.Debug("RabbitMQ Consumer cancel failed: %s", err.Error())
		return fmt.Errorf("RabbitMQ Consumer cancel failed: %s", err)
	}

	//close the connection of rabbitmq
	if err := c.connection.Close(); err != nil {
		log4go.Debug("RabbitMQ AMQP connection close error: %s", err.Error())
		return fmt.Errorf("RabbitMQ AMQP connection close error: %s", err)
	}

	defer log4go.Debug("RabbitMQ AMQP shutdown OK")

	return <-c.done // wait for handle() to exit
}
