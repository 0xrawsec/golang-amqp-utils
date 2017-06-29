package main

import (
	"consumer"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/0xrawsec/amqp"
	"github.com/0xrawsec/golang-utils/args"
	"github.com/0xrawsec/golang-utils/log"
)

var (
	debug                   bool
	queueName, exchangeName string
	timeout                 args.DurationVar

	exchangeType   = amqp.ExchangeFanout
	consumerConfig = consumer.Config{
		AmqpURL: os.Getenv("AMQP_URL"),
		Workers: 1}
)

// Function that just print the body of the of the message
func printBody(d *amqp.Delivery) interface{} {
	fmt.Println(string(d.Body))
	return nil
}

func main() {
	flag.BoolVar(&debug, "d", debug, "Enable debugging")
	flag.StringVar(&queueName, "q", queueName, "The name of the queue to be created"+
		"(it is mandatory if on publisher side you do not publish to an exchange)")
	flag.StringVar(&exchangeName, "e", exchangeName, "The exchange to be created")
	flag.StringVar(&exchangeType, "ex-type", exchangeType, "Type of the exchange to be created")
	flag.Var(&timeout, "t", "Timeout for consumer")
	flag.Parse()

	if debug {
		log.InitLogger(log.LDebug)
	}

	q := consumer.TemporaryQueue(queueName)
	c := consumer.NewBasicConsumer(&consumerConfig, &q)
	//c.Consume(consumer.DeliveryLogHandler, false)
	c.Consume(printBody, false)
	if time.Duration(timeout) > 0 {
		go func() {
			time.Sleep(time.Duration(timeout))
			c.Shutdown(false)
		}()
	}
	c.Wait()
}
