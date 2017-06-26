package publisher

/*
Properties:
	- publisher should not define exchange
	- publisher should not define queue
	- publisher should only be able to publish messages to queue/exchange
*/

import (
	"encoding/json"

	"github.com/0xrawsec/golang-utils/log"
	"github.com/streadway/amqp"
)

// Config structure for Publisher
type Config struct {
	AmqpURL string
}

// Publisher structure
type Publisher struct {
	Config       *Config
	conn         *amqp.Connection
	channel      *amqp.Channel
	confirmation chan amqp.Confirmation
}

// NewBasicPublisher returns a basic forwarder (no ampq authentication)
func NewBasicPublisher(config *Config) (p Publisher) {
	var err error
	p.Config = config
	p.conn, err = amqp.Dial(config.AmqpURL)
	if err != nil {
		panic(err)
	}
	// Notification when the connection closes
	go func() {
		log.Infof("Closing connection: %s", <-p.conn.NotifyClose(make(chan *amqp.Error)))
	}()
	p.channel, err = p.conn.Channel()
	if err != nil {
		log.LogErrorAndExit(err)
	}
	go func() {
		log.Infof("Closing channel: %s", <-p.channel.NotifyClose(make(chan *amqp.Error)))
	}()

	return
}

// PublishKey publishes a message to a Queue only
func (p *Publisher) PublishKey(key string, mandatory, immediate bool, msg amqp.Publishing) error {
	if err := p.channel.Publish("", key, mandatory, immediate, msg); err != nil {
		log.Errorf("Cannot forward: %s", err)
		return err
	}
	return nil
}

// PublishExchange publishes a message to an Exchange only
func (p *Publisher) PublishExchange(eName string, mandatory, immediate bool, msg amqp.Publishing) error {
	if err := p.channel.Publish(eName, "", mandatory, immediate, msg); err != nil {
		log.Errorf("Cannot forward: %s", err)
		return err
	}
	return nil
}

// Publish publishes a msg into the appropriate exchange and Queue
func (p *Publisher) Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	if exchange != "" {
		return p.PublishExchange(exchange, mandatory, immediate, msg)
	}
	if key != "" {
		return p.PublishKey(key, mandatory, immediate, msg)
	}
	if key == "" && exchange == "" {
		return p.PublishKey(key, mandatory, immediate, msg)
	}
	return nil
}

// Close closes the publisher
func (p *Publisher) Close() error {
	return p.conn.Close()
}

// BasicJSONPublishing creates a simple amqp.Publishing containing a JSON document
func BasicJSONPublishing(data interface{}) (p amqp.Publishing, err error) {
	jb, err := json.Marshal(data)
	if err != nil {
		return
	}
	p = amqp.Publishing{
		Headers:         amqp.Table{},
		ContentType:     "application/json",
		ContentEncoding: "",
		Body:            jb,
		DeliveryMode:    amqp.Persistent,
		Priority:        0,
	}
	return
}