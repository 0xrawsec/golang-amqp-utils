package main

import (
	"amqpconfig"
	"consumer"
	"fmt"
	"os"
	"publisher"
	"testing"
	"time"

	"github.com/0xrawsec/amqp"
	"github.com/0xrawsec/golang-utils/log"
)

func init() {
	log.InitLogger(log.LDebug)
}

var (
	consumerConfig = amqpconfig.Config{
		AmqpURL: os.Getenv("AMQP_URL"),
		Workers: 10}

	forwarderConfig = amqpconfig.Config{
		AmqpURL: os.Getenv("AMQP_URL")}

	evtxFile = "sysmon.evtx"
)

func TestBasic(t *testing.T) {
	q := consumer.TemporaryQueue("TestBasic")
	c := consumer.NewConsumer(&consumerConfig, &q)
	defer c.Shutdown(false)
	c.Consume(consumer.DeliveryLogHandler, false)
	f := publisher.NewPublisher(&forwarderConfig)
	for i := 0; i <= 100; i++ {
		msg, err := publisher.BasicJSONPublishing(fmt.Sprintf("test-%d", i))
		if err != nil {
			panic(err)
		}
		f.Publish("", q.Name, false, false, msg)
	}

	time.Sleep(3 * time.Second)
}

func TestBasicWResult(t *testing.T) {
	conf := consumerConfig
	conf.Workers = 1
	q := consumer.TemporaryQueue("TestBasicWResult")
	c := consumer.NewConsumer(&conf, &q)
	c.Consume(consumer.DeliveryGetBody, true)
	f := publisher.NewPublisher(&forwarderConfig)
	for i := 0; i <= 100; i++ {
		msg, err := publisher.BasicJSONPublishing(fmt.Sprintf("test-%d", i))
		if err != nil {
			panic(err)
		}
		f.Publish("", q.Name, false, false, msg)
	}
	go func() {
		time.Sleep(3 * time.Second)
		c.Shutdown(false)
	}()

	for i := range c.OutputChan {
		t.Logf("type:%T value:%[1]v", i)
	}

}
func TestBasicExclusive(t *testing.T) {
	conf := consumerConfig
	conf.Workers = 1
	q := consumer.TemporaryQueue("TestBasicExclusive")
	c := consumer.NewConsumer(&conf, &q)
	c.SetExclusive(true)
	defer c.Shutdown(false)
	c.Consume(consumer.DeliveryLogHandler, false)
	f := publisher.NewPublisher(&forwarderConfig)
	for i := 0; i <= 100; i++ {
		msg, err := publisher.BasicJSONPublishing(fmt.Sprintf("test-%d", i))
		if err != nil {
			panic(err)
		}
		f.Publish("", q.Name, false, false, msg)
	}
	time.Sleep(3 * time.Millisecond)
	c.Abort()

	time.Sleep(3 * time.Second)
}

func TestTwoConsumers(t *testing.T) {
	q := consumer.TemporaryQueue("TestTwoConsumers")
	conf := consumerConfig
	conf.Workers = 1
	c1 := consumer.NewConsumer(&conf, &q)
	c1.SetTag("Consumer-1")
	c2 := consumer.NewConsumer(&conf, &q)
	c2.SetWorkers(2)
	c2.SetTag("Consumer-2")
	defer c1.Shutdown(false)
	defer c2.Shutdown(false)
	c1.Consume(consumer.DeliveryLogHandler, false)
	c2.Consume(consumer.DeliveryLogHandler, false)

	// Publish into the queue
	f := publisher.NewPublisher(&forwarderConfig)
	for i := 0; i <= 100; i++ {
		msg, err := publisher.BasicJSONPublishing(fmt.Sprintf("test-%d", i))
		if err != nil {
			panic(err)
		}
		f.Publish("", q.Name, false, false, msg)
	}

	time.Sleep(3 * time.Second)
}

func TestBasicWExchange(t *testing.T) {
	q := consumer.TemporaryQueue("TestBasic")
	e := consumer.TemporaryExchange("TestBasicWExchange", amqp.ExchangeFanout)
	c := consumer.NewConsumer(&consumerConfig, &q)
	defer c.Shutdown(false)
	c.Bind(e)
	c.Consume(consumer.DeliveryLogHandler, false)
	f := publisher.NewPublisher(&forwarderConfig)
	for i := 0; i <= 100; i++ {
		msg, err := publisher.BasicJSONPublishing(fmt.Sprintf("test-%d", i))
		if err != nil {
			panic(err)
		}
		f.Publish(e.Name, "", false, false, msg)
	}
	time.Sleep(3 * time.Second)
}

func TestTwoConsumersWExchange(t *testing.T) {
	q := consumer.TemporaryQueue("")
	e := consumer.TemporaryExchange("TestBasicWExchange", amqp.ExchangeFanout)
	conf := consumerConfig
	conf.Workers = 1
	c1 := consumer.NewConsumer(&conf, &q)
	c1.SetTag("Consumer-1")
	c2 := consumer.NewConsumer(&conf, &q)
	c2.SetWorkers(2)
	c2.SetTag("Consumer-2")
	defer c1.Shutdown(false)
	defer c2.Shutdown(false)
	c1.Bind(e)
	c2.Bind(e)
	c1.Consume(consumer.DeliveryLogHandler, false)
	c2.Consume(consumer.DeliveryLogHandler, false)

	// Publish into the queue
	f := publisher.NewPublisher(&forwarderConfig)
	for i := 0; i <= 100; i++ {
		msg, err := publisher.BasicJSONPublishing(fmt.Sprintf("test-%d", i))
		if err != nil {
			panic(err)
		}
		f.Publish(e.Name, "", false, false, msg)
	}

	time.Sleep(3 * time.Second)
}

func TestTLSConsumer(t *testing.T) {
	amqpConf, err := amqpconfig.LoadConfigFromFile("config.json")
	if err != nil {
		t.Log("Failed to load consumer conf")
		t.FailNow()
	}
	q := consumer.TemporaryQueue("TestTLSConsumer")
	c := consumer.NewConsumer(&amqpConf, &q)
	c.Consume(consumer.DeliveryGetBody, true)
	f := publisher.NewPublisher(&amqpConf)
	for i := 0; i <= 100; i++ {
		msg, err := publisher.BasicJSONPublishing(fmt.Sprintf("test-%d", i))
		if err != nil {
			panic(err)
		}
		f.Publish("", q.Name, false, false, msg)
	}
	go func() {
		time.Sleep(3 * time.Second)
		c.Shutdown(false)
	}()

	for i := range c.OutputChan {
		t.Logf("type:%T value:%[1]v", i)
	}

}
