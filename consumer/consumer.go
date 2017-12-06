package consumer

/*
Properties:
	- The consumer is responsible for creating queues/exchanges
	- The consumer can consume only from one queue
	- The consumer can have several workers consuming from the queue
*/

import (
	"amqpconfig"
	"fmt"

	"github.com/0xrawsec/amqp"
	"github.com/0xrawsec/golang-utils/log"
)

var (
	// ErrNilConsumerQueue returned error when consumer queue is Nil
	ErrNilConsumerQueue = fmt.Errorf("Nil Consumer Queue")
)

// Queue structure
type Queue struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       amqp.Table
}

// TemporaryQueue helper to create a temporary queue
func TemporaryQueue(name string) Queue {
	return Queue{name, false, true, false, false, nil}
}

// Exchange structure
type Exchange struct {
	Name       string
	Type       string
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Args       amqp.Table
	//NumQueues   int
	QueueConfig Queue
}

// TemporaryExchange initialize a temporary exchange
func TemporaryExchange(name, eType string) (e Exchange) {
	e.Name = name
	e.Type = eType
	e.Durable = false
	e.AutoDelete = true
	e.Internal = false
	e.NoWait = false
	e.Args = nil
	return
}

// String implementation
func (e *Exchange) String() string {
	return fmt.Sprintf("Name: %s Type:%s Durable:%t AutoDelete:%t Internal:%t NoWait:%t Args:%s",
		e.Name, e.Type, e.Durable, e.AutoDelete, e.Internal, e.NoWait, e.Args)
}

// Consumer structure
type Consumer struct {
	Config     *amqpconfig.Config
	Queue      *amqp.Queue
	OutputChan chan interface{}
	conn       *amqp.Connection
	channel    *amqp.Channel
	tag        string
	noAck      bool
	exclusive  bool
	noLocal    bool
	noWait     bool
	args       amqp.Table
	workers    int
	abort      bool
	done       chan error
	running    bool
}

// NewConsumer creates a new consumer not using TLS
func NewConsumer(config *amqpconfig.Config, q *Queue) (c Consumer, err error) {
	c.Config = config

	c.workers = c.Config.Workers
	if c.workers <= 0 {
		c.workers = 1
	}

	c.OutputChan = make(chan interface{}, c.workers)

	c.conn, err = amqp.DialTLS(config.AmqpURL, &(config.TLSConf))
	if err != nil {
		return
	}

	// Notification when the connection closes
	go func() {
		log.Infof("Closing connection: %s", <-c.conn.NotifyClose(make(chan *amqp.Error)))
	}()

	c.channel, err = c.conn.Channel()
	if err != nil {
		return
	}

	go func() {
		log.Infof("Closing channel: %s", <-c.channel.NotifyClose(make(chan *amqp.Error)))
	}()

	// Set the queue if it is not nil
	if q != nil {
		err = c.SetQueue(q)
		if err != nil {
			return
		}
	}

	// Initializing the channel
	c.done = make(chan error)

	// Running state initiazed
	c.running = true

	return
}

// SetTag tag setter
func (c *Consumer) SetTag(tag string) {
	c.tag = tag
}

// SetNoAck noAck setter
func (c *Consumer) SetNoAck(noAck bool) {
	c.noAck = noAck
}

// SetExclusive exclusive setter
func (c *Consumer) SetExclusive(excl bool) {
	c.exclusive = excl
}

// SetNoLocal noLocal setter
func (c *Consumer) SetNoLocal(noloc bool) {
	c.noLocal = noloc
}

// SetNoWait noWait setter
func (c *Consumer) SetNoWait(noWait bool) {
	c.noWait = noWait
}

// SetArgs args setter
func (c *Consumer) SetArgs(args amqp.Table) {
	c.args = args
}

// SetWorkers sets the number of concurrent workers
func (c *Consumer) SetWorkers(workers int) {
	if workers >= 1 {
		c.workers = workers
	}
}

// SetQueue helps to split up the code to set the queue of the consumer
func (c *Consumer) SetQueue(q *Queue) error {
	var err error
	queue, err := c.channel.QueueDeclare(q.Name,
		q.Durable,
		q.AutoDelete,
		q.Exclusive,
		q.NoWait,
		q.Args)
	if err != nil {
		log.Errorf("Cannot declare queue: %s", err)
		return err
	}
	log.Infof("Queue successfully declared: %s", queue.Name)
	c.Queue = &queue
	return nil
}

// Abort tells the conusmer to stop consuming
func (c *Consumer) Abort() {
	c.abort = true
}

// Flush the OutputChan of the conusmer to prevent deadlocks
func (c *Consumer) Flush() {
	go func() {
		for range c.OutputChan {
		}
	}()
}

// Bind binds the queue of the Consumer to the exchange
func (c *Consumer) Bind(e Exchange) error {
	log.Debugf("Initializing Exchange: %s", &e)
	if c.Queue == nil {
		log.Debugf("Cannot bind on nil queue (Consumer queue needs to be set before)")
		return ErrNilConsumerQueue
	}
	if err := c.channel.ExchangeDeclare(e.Name, e.Type, e.Durable, e.AutoDelete, e.Internal, e.NoWait, e.Args); err != nil {
		return err
	}
	if err := c.channel.QueueBind(c.Queue.Name, "", e.Name, false, nil); err != nil {
		log.Debugf("Cannot bind Exchange:%s <-> Queue:%s", e.Name, c.Queue.Name)
		return err
	}
	return nil
}

// Consume function consumes for any queue from the consumer
func (c *Consumer) Consume(f func(*amqp.Delivery) interface{}, expectResult bool) error {
	deliveries, err := c.channel.Consume(
		c.Queue.Name, // name
		c.tag,        // consumerTag,
		c.noAck,      // noAck
		c.exclusive,  // exclusive
		c.noLocal,    // noLocal
		c.noWait,     // noWait
		c.args,       // arguments
	)

	if err != nil {
		log.Debugf("Cannot Consume from %s: %s", c.Queue.Name, err)
		return err
	}

	for i := 0; i < c.workers; i++ {
		log.Debugf("Starting worker %d", i)
		//i := ConsumerInfo{c.tag, i}
		//go handler(c, i, deliveries, c.done)
		go c.Work(i, deliveries, f, expectResult)
	}
	return nil
}

// Work processes the deliveries one by one by applying function f on each
func (c *Consumer) Work(workerID int, deliveries <-chan amqp.Delivery, f func(d *amqp.Delivery) interface{}, expectResult bool) {
	log.Debug("Starting BasicHandler")
	for d := range deliveries {
		// Check if the consumer is aborted
		if c.abort {
			log.Debugf("Aborting handler for Consumer:\"%s\" worker:%d", c.tag, workerID)
			c.done <- nil
			return
		}
		// Apply the function on the delivery
		log.Debugf("Processing delivery for Consumer:\"%s\" worker:%d", c.tag, workerID)
		if expectResult {
			c.OutputChan <- f(&d)
		} else {
			f(&d)
		}
		d.Ack(false)
	}
	log.Infof("BasicHandler: deliveries channel closed")
	c.done <- nil
}

// Wait the workers of the consumer
func (c *Consumer) Wait() {
	defer close(c.done)
	if c.running {
		log.Debug("Waiting all the workers to finish")
		for i := 0; i < c.workers; i++ {
			e := <-c.done
			if e != nil {
				log.Errorf("Worker finished with error: %s", e)
			} else {
				log.Debugf("Worker finished %d/%d", i+1, c.workers)
			}
		}
		log.Infof("All the %d workers are done", c.workers)
	}
	// It is not running anymore
	c.running = false
}

// Shutdown function
func (c *Consumer) Shutdown(flush bool) error {
	// Anyway close the channels hold by the Consumer
	defer close(c.OutputChan)
	// Purge Queue if it exclusive Consumer
	if c.exclusive {
		log.Debug("Purging the remaining entries in the queue")
		if _, err := c.channel.QueuePurge(c.Queue.Name, false); err != nil {
			log.Errorf("Failed to purge queue: %s", c.Queue.Name)
		}
	}
	// will close() the deliveries channel
	if err := c.channel.Cancel(c.tag, true); err != nil {
		return fmt.Errorf("Consumer cancel failed: %s", err)
	}

	if err := c.conn.Close(); err != nil {
		return fmt.Errorf("AMQP connection close error: %s", err)
	}

	defer log.Info("AMQP shutdown OK")

	// Waiting the workers
	//c.Wait()

	// Flushing the output to prevent deadlocks
	if flush {
		c.Flush()
	}
	// Wait for all the workers
	return nil
}

// DeliveryLogHandler is an Helper to log the delivery
func DeliveryLogHandler(d *amqp.Delivery) interface{} {
	log.Infof(
		"Got %dB delivery: [%v] %q",
		len(d.Body),
		d.DeliveryTag,
		d.Body,
	)
	return nil
}

// DeliveryGetBody is a Helper that just return the body of the delively as a
// string
func DeliveryGetBody(d *amqp.Delivery) interface{} {
	return string(d.Body)
}
