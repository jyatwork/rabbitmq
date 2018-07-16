package main

import (
	"fmt"
	"sync"

	"conf_yaml/conf"
	"rabbitmq/rabbit"

	"github.com/streadway/amqp"
)

func initConfigs() {

	conf.Load("RabbitMQ.yaml")

}

func main() {
	initConfigs()

	var wg sync.WaitGroup

	connSucessFlag := make(chan int, 1)
	rbConn, err := rabbit.GetMQConn(conf.String("rabbitmq.uri"), connSucessFlag)
	if err != nil {
		fmt.Println("RabbitMQ GetMQConn:", err.Error())
	}
	wg.Add(1)

	go func() {
		for {
			<-rbConn.ConnFlag

			go consume(rbConn)
		}
	}()
	wg.Add(1)

	wg.Wait()
}

func consume(rbConn *rabbit.RbConn) {

	exchange := conf.String("rabbitmq.exchange.0.name")
	exchangeType := conf.String("rabbitmq.exchange.0.type")
	routingKey := conf.String("rabbitmq.queue.0.key")
	queue := conf.String("rabbitmq.queue.0.name")
	consumerTag := conf.String("rabbitmq.consumerTag")

	//initialize rabbitmq publisher
	rabbit.InitPublisher("rabbitmq_publisher")
	fmt.Println("RabbitMQ: publisher params initialize ok")
	c, err := rabbit.NewConsumer(rbConn.RabbitConn, exchange, exchangeType, queue, routingKey, consumerTag, handlerData)
	if err != nil {
		fmt.Println("RabbitMQ NewConsumer:", err.Error())
	}
	select {}

	fmt.Println("RabbitMQ shutting down")

	if err := c.Shutdown(); err != nil {
		fmt.Println("RabbitMQ consume shutdown: ", err)
	}
}

//handlerData 开始处理rabbitmq收到的数据.
func handlerData(d amqp.Delivery) {

	var err error

	//parsed something
	//ack
	if err != nil {
		fmt.Println(fmt.Sprintf("RabbitMQ handlerData Error: %s", err.Error()))

		//Ack掉
		d.Ack(false)

	} else {
		//Ack掉
		d.Ack(false)
	}
}
