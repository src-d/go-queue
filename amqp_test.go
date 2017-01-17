package queue

import (
	"errors"
	"testing"
	"time"

	. "gopkg.in/check.v1"
	"gopkg.in/mgo.v2/bson"
)

func Test(t *testing.T) { TestingT(t) }

const amqpURL = "amqp://guest:guest@localhost:5672/"

type AMQPSuite struct {
	broker Broker
}

var _ = Suite(&AMQPSuite{})

func (s *AMQPSuite) SetUpSuite(c *C) {
	b, err := NewAMQPBroker(amqpURL)
	c.Assert(err, IsNil)
	s.broker = b
}

func (s *AMQPSuite) TearDownSuite(c *C) {
	c.Assert(s.broker.Close(), IsNil)
}

func (s *AMQPSuite) TestPublishAndConsume(c *C) {
	q, err := s.broker.Queue(bson.NewObjectId().Hex())
	c.Assert(err, IsNil)

	job := NewJob()
	job.Encode(true)
	err = q.Publish(job)
	c.Assert(err, IsNil)

	for i := 0; i < 100; i++ {
		job := NewJob()
		job.Encode(i)
		err = q.Publish(job)
		c.Assert(err, IsNil)
	}

	i, err := q.Consume()
	c.Assert(err, IsNil)

	retrievedJob, err := i.Next()
	c.Assert(err, IsNil)
	c.Assert(retrievedJob.Ack(), IsNil)

	var payload bool
	err = retrievedJob.Decode(&payload)
	c.Assert(err, IsNil)
	c.Assert(payload, Equals, true)

	c.Assert(retrievedJob.ID, Equals, job.ID)
	c.Assert(retrievedJob.Priority, Equals, job.Priority)
	c.Assert(retrievedJob.Timestamp.Second(), Equals, job.Timestamp.Second())

	for k := 0; k < 100; k++ {
		j, err := i.Next()
		c.Assert(err, IsNil)
		if j == nil {
			break
		}

		c.Assert(j.Ack(), IsNil)
		var payload int
		c.Assert(j.Decode(&payload), IsNil)
		c.Assert(payload, Equals, k)
	}

	err = i.Close()
	c.Assert(err, IsNil)
}

func (s *AMQPSuite) TestDelayed(c *C) {
	q, err := s.broker.Queue(bson.NewObjectId().Hex())
	c.Assert(err, IsNil)

	job := NewJob()
	job.Encode("hello")
	err = q.PublishDelayed(job, 1*time.Second)
	c.Assert(err, IsNil)

	i, err := q.Consume()
	c.Assert(err, IsNil)

	start := time.Now()
	var since time.Duration
	for {
		j, err := i.Next()
		c.Assert(err, IsNil)
		if j == nil {
			<-time.After(300 * time.Millisecond)
			continue
		}

		since = time.Since(start)

		var payload string
		c.Assert(j.Decode(&payload), IsNil)
		c.Assert(payload, Equals, "hello")
		break
	}

	c.Assert(since >= 1*time.Second, Equals, true)
}

func (s *AMQPSuite) TestTransaction(c *C) {
	q, err := s.broker.Queue(bson.NewObjectId().Hex())
	c.Assert(err, IsNil)

	err = q.Transaction(func(qu Queue) error {
		job := NewJob()
		c.Assert(job.Encode("goodbye"), IsNil)
		c.Assert(qu.Publish(job), IsNil)
		return errors.New("foo")
	})
	c.Assert(err, Not(IsNil))

	i, err := q.Consume()
	c.Assert(err, IsNil)
	go func() {
		j, err := i.Next()
		c.Assert(err, IsNil)
		c.Assert(j, IsNil)
	}()
	<-time.After(50 * time.Millisecond)
	c.Assert(i.Close(), IsNil)

	q, err = s.broker.Queue(bson.NewObjectId().Hex())
	c.Assert(err, IsNil)

	err = q.Transaction(func(q Queue) error {
		job := NewJob()
		c.Assert(job.Encode("hello"), IsNil)
		c.Assert(q.Publish(job), IsNil)
		return nil
	})
	c.Assert(err, IsNil)

	iter, err := q.Consume()
	c.Assert(err, IsNil)
	j, err := iter.Next()
	c.Assert(err, IsNil)
	c.Assert(j, Not(IsNil))
	var payload string
	c.Assert(j.Decode(&payload), IsNil)
	c.Assert(payload, Equals, "hello")
	c.Assert(iter.Close(), IsNil)
}
