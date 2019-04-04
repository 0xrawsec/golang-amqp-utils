package main

import (
	"compress/gzip"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/0xrawsec/amqp"
	humanize "github.com/0xrawsec/go-humanize"
	"github.com/0xrawsec/golang-amqp-utils/amqpconfig"
	"github.com/0xrawsec/golang-amqp-utils/consumer"
	"github.com/0xrawsec/golang-utils/args"
	"github.com/0xrawsec/golang-utils/log"
)

var (
	debug                   bool
	queueName, exchangeName string
	output                  string
	configPath              string
	timeout                 args.DurationVar
	outputSizeRotate        uint64

	exchangeType   = amqp.ExchangeFanout
	consumerConfig = amqpconfig.Config{
		AmqpURL: os.Getenv("AMQP_URL"),
		Workers: 1,
		TLSConf: tls.Config{}}

	retryTimeout = time.Second * 10
	// Stdout by default
	writer io.Writer = os.Stdout
	mutex            = sync.Mutex{}

	strOutputSizeRotate = "500MB"
)

// Function that just print the body of the of the message
func printBody(d *amqp.Delivery) interface{} {
	// gzip Writer does not seem to support parallel write operations so we force it
	mutex.Lock()
	defer mutex.Unlock()
	fmt.Fprintf(writer, "%s\n", string(d.Body))
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
	flag.StringVar(&strOutputSizeRotate, "size", strOutputSizeRotate, "Rotate output file when size is reached")
	flag.Var(&timeout, "t", "Timeout for consumer")
	flag.Parse()

	if debug {
		log.InitLogger(log.LDebug)
	}

	outputSizeRotate, err := humanize.ParseBytes(strOutputSizeRotate)
	if err != nil {
		log.LogErrorAndExit(err)
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
			sig := <-s
			mutex.Lock()
			defer mutex.Unlock()
			log.Infof("Handling signal %s properly", sig)
			writer.(*gzip.Writer).Flush()
			writer.(*gzip.Writer).Close()
			f.Close()
			os.Exit(0)
		}()

		// Log rolling routine
		go func() {
			i := 1
			currentOutput := output
			for {
				stat, err := os.Stat(currentOutput)
				if err != nil {
					log.LogErrorAndExit(err)
				}

				if stat.Size() >= int64(outputSizeRotate) {
					mutex.Lock()
					writer.(*gzip.Writer).Flush()
					writer.(*gzip.Writer).Close()
					f.Close()
					currentOutput = fmt.Sprintf("%s.%d", output, i)
					f, err = os.Create(currentOutput)
					if err != nil {
						log.LogErrorAndExit(err)
					}
					writer = gzip.NewWriter(f)
					log.Infof("Output rotated: %s", currentOutput)
					mutex.Unlock()
					i++
				}

				time.Sleep(5 * time.Second)
			}
		}()
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
	c, err := consumer.NewConsumer(&consumerConfig, &q)

ConsumerErrorCheck:
	if err != nil {
		switch err.(type) {
		case net.Error:
			log.Errorf("Network error trying again in %s: %s", retryTimeout, err)
			c, err = consumer.NewConsumer(&consumerConfig, &q)
			goto ConsumerErrorCheck
		default:
			log.LogErrorAndExit(err)
		}
	}

	// If there is an exchange
	if exchangeName != "" {
		exchange := consumer.TemporaryExchange(exchangeName, exchangeType)
		c.Bind(exchange)
	}

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
