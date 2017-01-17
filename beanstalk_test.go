package queue

import (
	"time"

	. "gopkg.in/check.v1"
	"gopkg.in/mgo.v2/bson"
)

type BeanstalkSuite struct {
	broker Broker
}

var _ = Suite(&BeanstalkSuite{})

func (s *BeanstalkSuite) SetUpSuite(c *C) {
	b, err := NewBeanstalkBroker("127.0.0.1:11300")
	c.Assert(err, IsNil)
	s.broker = b
}

func (s *BeanstalkSuite) TearDownSuite(c *C) {
	c.Assert(s.broker.Close(), IsNil)
}

func (s *BeanstalkSuite) TestPublishAndConsume(c *C) {
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

	c.Assert(retrievedJob.tag, Equals, job.tag)
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

	_, err = i.Next()
	c.Assert(err, Not(IsNil))

	err = i.Close()
	c.Assert(err, IsNil)
}

func (s *BeanstalkSuite) TestDelayed(c *C) {
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
