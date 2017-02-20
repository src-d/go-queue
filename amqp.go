package queue

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/streadway/amqp"
	log15 "gopkg.in/inconshreveable/log15.v2"
)

var consumerSeq uint64

type AMQPBroker struct {
	mut        sync.RWMutex
	conn       *amqp.Connection
	ch         *amqp.Channel
	connErrors chan *amqp.Error
	stop       chan struct{}
}

type connection interface {
	connection() *amqp.Connection
	channel() *amqp.Channel
}

func NewAMQPBroker(url string) (Broker, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %s", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to open a channel: %s", err)
	}

	b := &AMQPBroker{
		conn: conn,
		ch:   ch,
		stop: make(chan struct{}),
	}

	go b.manageConnection(url)

	return b, nil
}

func connect(url string) (*amqp.Connection, *amqp.Channel) {
	// first try to connect again
	var conn *amqp.Connection
	var err error
	for {
		conn, err = amqp.Dial(url)
		if err != nil {
			log15.Error("error connecting to amqp", "err", err)
			<-time.After(1 * time.Second)
			continue
		}

		break
	}

	// try to get the channel again
	var ch *amqp.Channel
	for {
		ch, err = conn.Channel()
		if err != nil {
			log15.Error("error creatting channel", "err", err)
			<-time.After(1 * time.Second)
			continue
		}

		break
	}

	return conn, ch
}

func (b *AMQPBroker) manageConnection(url string) {
	b.connErrors = make(chan *amqp.Error)
	b.conn.NotifyClose(b.connErrors)

	for {
		select {
		case err := <-b.connErrors:
			log15.Error("amqp connection error", "err", err)
			b.mut.Lock()
			if err != nil {
				b.conn, b.ch = connect(url)

				b.connErrors = make(chan *amqp.Error)
				b.conn.NotifyClose(b.connErrors)
			}
			b.mut.Unlock()
		case <-b.stop:
			return
		}
	}
}

func (b *AMQPBroker) connection() *amqp.Connection {
	b.mut.Lock()
	defer b.mut.Unlock()
	return b.conn
}

func (b *AMQPBroker) channel() *amqp.Channel {
	b.mut.Lock()
	defer b.mut.Unlock()
	return b.ch
}

func (b *AMQPBroker) Queue(name string) (Queue, error) {
	q, err := b.ch.QueueDeclare(
		name,  // name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)

	if err != nil {
		return nil, err
	}

	return &AMQPQueue{conn: b, queue: q}, nil
}

func (b *AMQPBroker) Close() error {
	close(b.stop)
	if err := b.channel().Close(); err != nil {
		return err
	}

	if err := b.connection().Close(); err != nil {
		return err
	}

	return nil
}

type AMQPQueue struct {
	conn  connection
	queue amqp.Queue
}

func (q *AMQPQueue) Publish(j *Job) error {
	if j == nil || len(j.raw) == 0 {
		return ErrEmptyJob
	}

	return q.conn.channel().Publish(
		"",           // exchange
		q.queue.Name, // routing key
		false,        // mandatory
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			MessageId:    j.ID,
			Priority:     uint8(j.Priority),
			Timestamp:    j.Timestamp,
			ContentType:  string(j.contentType),
			Body:         j.raw,
		},
	)
}

func (q *AMQPQueue) PublishDelayed(j *Job, delay time.Duration) error {
	if j == nil || len(j.raw) == 0 {
		return ErrEmptyJob
	}

	ttl := delay / time.Millisecond
	delayedQueue, err := q.conn.channel().QueueDeclare(
		j.ID,  // name
		true,  // durable
		true,  // delete when unused
		false, // exclusive
		false, // no-wait
		amqp.Table{
			"x-dead-letter-exchange":    "",
			"x-dead-letter-routing-key": q.queue.Name,
			"x-message-ttl":             int64(ttl),
			"x-expires":                 int64(ttl) * 2,
		},
	)
	if err != nil {
		return err
	}

	return q.conn.channel().Publish(
		"",
		delayedQueue.Name,
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			MessageId:    j.ID,
			Priority:     uint8(j.Priority),
			Timestamp:    j.Timestamp,
			ContentType:  string(j.contentType),
			Body:         j.raw,
		},
	)
}

func (q *AMQPQueue) Transaction(txcb TxCallback) error {
	ch, err := q.conn.connection().Channel()
	if err != nil {
		return fmt.Errorf("failed to open a channel: %s", err)
	}
	defer ch.Close()

	if err := ch.Tx(); err != nil {
		return err
	}

	txQueue := &AMQPQueue{
		conn: &AMQPBroker{
			conn: q.conn.connection(),
			ch:   ch,
		},
		queue: q.queue,
	}

	err = txcb(txQueue)
	if err != nil {
		if err := ch.TxRollback(); err != nil {
			return err
		}
		return err
	}

	if err := ch.TxCommit(); err != nil {
		return err
	}

	return nil
}

func (q *AMQPQueue) Consume() (JobIter, error) {
	ch, err := q.conn.connection().Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to open a channel: %s", err)
	}

	// enforce prefetching only one job, if this is removed the whole queue
	// will be consumed.
	if err := ch.Qos(1, 0, false); err != nil {
		return nil, err
	}

	id := q.consumeID()
	c, err := ch.Consume(q.queue.Name, id, false, false, false, false, nil)
	if err != nil {
		return nil, err
	}

	return &AMQPJobIter{id: id, ch: ch, c: c}, nil
}

func (q *AMQPQueue) consumeID() string {
	return fmt.Sprintf("%s-%s-%d",
		os.Args[0],
		q.queue.Name,
		atomic.AddUint64(&consumerSeq, 1),
	)
}

type AMQPJobIter struct {
	id string
	ch *amqp.Channel
	c  <-chan amqp.Delivery
}

func (i *AMQPJobIter) Next() (*Job, error) {
	d, ok := <-i.c
	if !ok {
		return nil, ErrAlreadyClosed
	}

	return fromDelivery(&d), nil
}

func (i *AMQPJobIter) Close() error {
	if err := i.ch.Cancel(i.id, false); err != nil {
		return err
	}

	return i.ch.Close()
}

type AMQPAcknowledger struct {
	ack amqp.Acknowledger
	id  uint64
}

func (a *AMQPAcknowledger) Ack() error {
	return a.ack.Ack(a.id, false)
}

func (a *AMQPAcknowledger) Reject(requeue bool) error {
	return a.ack.Reject(a.id, requeue)
}

func fromDelivery(d *amqp.Delivery) *Job {
	j := NewJob()
	j.ID = d.MessageId
	j.Priority = Priority(d.Priority)
	j.Timestamp = d.Timestamp
	j.contentType = contentType(d.ContentType)
	j.acknowledger = &AMQPAcknowledger{d.Acknowledger, d.DeliveryTag}
	j.tag = d.DeliveryTag
	j.raw = d.Body

	return j
}
