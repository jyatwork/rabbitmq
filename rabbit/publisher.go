/**
 * rabbitMQ 发布者
 * Created by pigsatwork on 13.04月.2018
 */
package rabbit

import (
	"fmt"
	"time"

	"conf_yaml/conf"

	"github.com/alecthomas/log4go"
	"github.com/streadway/amqp"
)

type Data struct {
	Mac         string
	DataPackage string
}

type DataStorage struct {
	conn     *amqp.Connection
	channel  *amqp.Channel
	Queue    string
	QueueKey string
	Exch     string
	ExchType string
	PackChan chan *Data
}

type Listener struct {
	//exchange:conn,channel
	Param map[string]*DataStorage
}

var (
	dataStorage *DataStorage
	listener    *Listener
)

func GetListener(mq string) *Listener {

	if listener == nil {
		listener, err := InitPublisher(mq)
		if err != nil {
			log4go.Error(err.Error())
			return nil
		}
		return listener
	}
	return listener
}

func InitPublisher(mq string) (*Listener, error) {
	tempMap := make(map[string]*DataStorage)
	for i := 0; i < conf.Int(mq+".num"); i++ {

		index := fmt.Sprintf("%d", i)
		queue := conf.String(mq + ".queue." + index + ".name")
		routingKey := conf.String(mq + ".queue." + index + ".key")
		exch := conf.String(mq + ".exchange." + index + ".name")
		exchType := conf.String(mq + ".exchange." + index + ".type")

		dataStorage = &DataStorage{PackChan: make(chan *Data, 100)}

		err := dataStorage.connect(mq, exch, exchType)
		if err != nil {
			//noinspection GoPlaceholderCount
			log4go.Error("RabbitMQ getMQStorage connect %s error: %s", exch, err.Error())
			return nil, err
		}

		go dataStorage.DataCollStart(mq, exch, exchType, queue, routingKey)

		tempMap[mq+":"+exch] = dataStorage
	}
	if listener != nil {
		for k, v := range tempMap {
			listener.Param[k] = v
		}
	} else {
		listener = &Listener{Param: tempMap}
	}

	return listener, nil
}

func (ds *DataStorage) DataCollStart(mq, exchangeName, exchangeType, queue, routingKey string) {
	var dat *Data
	for {
		select {
		case dat = <-ds.PackChan:
			//log4go.Debug("DataCollStart got data:%s, %s", dat.Mac, dat.DataPackage)
			ds.publish(mq, exchangeName, exchangeType, dat.DataPackage, routingKey+dat.Mac)

		}
	}
	ds.closeMQ()
}

func (ds *DataStorage) connect(mq, exchangeName, excType string) error {
	var err error

	ds.conn, err = amqp.Dial(conf.String(mq + ".uri"))
	if err != nil {
		//noinspection ALL
		log4go.Error("RabbitMQ dialing amqp failed: %s", err.Error())
		time.Sleep(1000 * time.Millisecond)
		return ds.connect(mq, exchangeName, excType)
	}

	ds.channel, err = ds.conn.Channel()
	if err != nil {
		//noinspection ALL
		log4go.Error("RabbitMQ got channel failed: %s", err.Error())
		time.Sleep(1000 * time.Millisecond)
		return ds.connect(mq, exchangeName, excType)
	}

	if er := ds.channel.ExchangeDeclare(
		exchangeName, excType,
		true,  // durable
		false, // auto-delete
		false, // internal
		false, // no-wait
		nil,   // args
	); er != nil {
		//noinspection ALL
		log4go.Error("RabbitMQ declaring %q Exchange (%q) failed: %s", excType, exchangeName, er.Error())
	}

	log4go.Debug("RabbitMQ connected. exchang:%s, exchType:%s", exchangeName, excType)

	return err
}

func (ds *DataStorage) closeMQ() {
	ds.channel.Close()
	ds.conn.Close()

}

//连接rabbitmq server
func (ds *DataStorage) publish(mq, exchangeName, exchangeType, msgContent, key string) {

	//log4go.Debug("RabbitMQ Publishing -> exchange:%s, exchType:%s, routingKey:%s, msg:%s \n", exchangeName, exchangeType, key, msgContent)
	log4go.Debug("RabbitMQ Publishing -> exchange:%s, exchType:%s, routingKey:%s\n", exchangeName, exchangeType, key)
	if ds.channel == nil {
		//重连
		ds.conn.Close()
		ds.connect(mq, exchangeName, exchangeType)
		return
	}

	if err := ds.channel.Publish(
		exchangeName, key, false, false,
		amqp.Publishing{
			ContentType:  "text/plain",
			DeliveryMode: amqp.Persistent,
			Body:         []byte(msgContent),
		}); err != nil {
		//noinspection GoPlaceholderCount
		log4go.Error("RabbitMQ publish to exchange: %s error: (%s) %s.\n", exchangeName, key, err.Error())
	}
}
