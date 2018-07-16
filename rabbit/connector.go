package rabbit

import (
	"time"

	"github.com/alecthomas/log4go"
	"github.com/streadway/amqp"
)

type RbConn struct {
	RabbitConn       *amqp.Connection
	RabbitCloseError chan *amqp.Error
	ConnFlag         chan int //mq connetcion notify
}

var (
	rbconn *RbConn
	uri    string
)

// Try to connect to the RabbitMQ server as long as it takes to establish a connection
func (c *RbConn) connectToRabbitMQ() {
	var err error

	for {
		c.RabbitConn, err = amqp.Dial(uri)

		if err == nil {
			log4go.Info("RabbitMQ connection established! %q", uri)
			c.ConnFlag <- 1
			break
		}

		//noinspection ALL
		log4go.Error("RabbitMQ connection crushed, %s ", err.Error())
		//log4go.Debug("RabbitMQ Trying to reconnect to %q", uri)
		time.Sleep(500 * time.Millisecond)
	}
}

// re-establish the connection to RabbitMQ in case the connection has died
func (c *RbConn) rabbitConnector() {
	var rabbitErr *amqp.Error
	for {

		rabbitErr = <-c.RabbitCloseError //connection closed error reader

		if rabbitErr != nil {

			c.connectToRabbitMQ()

			c.RabbitCloseError = make(chan *amqp.Error) //reinitialize the connection close error

			log4go.Debug("RabbitMQ Reconnection finnaly established!! ")
			c.RabbitConn.NotifyClose(c.RabbitCloseError) //reset the connection listener
		}
	}
}

func GetMQConn(rabbitUri string, connFlag chan int) (*RbConn, error) {
	uri = rabbitUri
	if rbconn == nil {
		rbconn = &RbConn{
			RabbitCloseError: make(chan *amqp.Error),
			ConnFlag:         connFlag,
		}

		rbconn.connectToRabbitMQ()

		rbconn.RabbitConn.NotifyClose(rbconn.RabbitCloseError) //set a connection error listener

		go rbconn.rabbitConnector() //goroutine about reconnect
	}

	return rbconn, nil
}
