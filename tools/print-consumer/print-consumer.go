package main

import (
	"amqpconfig"
	"compress/gzip"
	"consumer"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"time"

	"github.com/0xrawsec/amqp"
	"github.com/0xrawsec/golang-utils/args"
	"github.com/0xrawsec/golang-utils/log"
)

var (
	debug                   bool
	queueName, exchangeName string
	output                  string
	configPath              string
	timeout                 args.DurationVar

	exchangeType   = amqp.ExchangeFanout
	consumerConfig = amqpconfig.Config{
		AmqpURL: os.Getenv("AMQP_URL"),
		Workers: 1,
		TLSConf: tls.Config{}}

	// Stdout by default
	writer io.Writer = os.Stdout
)

// Function that just print the body of the of the message
func printBody(d *amqp.Delivery) interface{} {
	fmt.Fprintf(writer, string(d.Body))
	return nil
}

func main() {
	flag.BoolVar(&debug, "d", debug, "Enable debugging")
	flag.StringVar(&queueName, "q", queueName, "The name of the queue to be created"+
		"(it is mandatory if on publisher side you do not publish to an exchange)")
	flag.StringVar(&exchangeName, "e", exchangeName, "The exchange to be created")
	flag.StringVar(&exchangeType, "ex-type", exchangeType, "Type of the exchange to be created")
	flag.StringVar(&output, "o", output, "Dumps the result (gzipped) into a file instead of printing")
	flag.StringVar(&configPath, "c", configPath, "Path to configuration file")
	flag.Var(&timeout, "t", "Timeout for consumer")
	flag.Parse()

	if debug {
		log.InitLogger(log.LDebug)
	}

	if output != "" {
		f, err := os.Create(output)
		if err != nil {
			log.LogErrorAndExit(err)
		}
		writer = gzip.NewWriter(f)
		defer writer.(*gzip.Writer).Flush()
		defer writer.(*gzip.Writer).Close()
		defer f.Close()

		// We register clean signal handler
		s := make(chan os.Signal, 1)
		signal.Notify(s, os.Interrupt, os.Kill)
		go func() {
			<-s
			writer.(*gzip.Writer).Flush()
			writer.(*gzip.Writer).Close()
			f.Close()
		}()
	}

	q := consumer.TemporaryQueue(queueName)
	c := consumer.NewBasicConsumer(&consumerConfig, &q)

	// If there is an exchange
	if exchangeName != "" {
		exchange := consumer.TemporaryExchange(exchangeName, exchangeType)
		c.Bind(exchange)
	}

	// We consume the messages and process those with printBody method
	if configPath != "" {
		var err error
		consumerConfig, err = amqpconfig.LoadConfigFromFile(configPath)
		if err != nil {
			log.LogErrorAndExit(fmt.Errorf("Failed to load configuration: %s", err))
		}
	}

	q := consumer.TemporaryQueue(queueName)
	c := consumer.NewConsumer(&consumerConfig, &q)
	//c.Consume(consumer.DeliveryLogHandler, false)
	c.Consume(printBody, false)

	// We kill the consumer after a given amount of time
	if time.Duration(timeout) > 0 {
		go func() {
			time.Sleep(time.Duration(timeout))
			c.Shutdown(false)
		}()
	}
	c.Wait()
}
