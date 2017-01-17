package queue

import (
	"fmt"
	"time"

	. "gopkg.in/check.v1"
	"gopkg.in/mgo.v2/bson"
)

var _ = Suite(&MemorySuite{})

type MemorySuite struct{}

func (s *MemorySuite) TestIntegration(c *C) {
	b := NewMemoryBroker()

	q, err := b.Queue(bson.NewObjectId().Hex())
	c.Assert(err, IsNil)

	job := NewJob()
	job.Encode(true)
	err = q.Publish(job)
	c.Assert(err, IsNil)

	for i := 0; i < 100; i++ {
		job := NewJob()
		job.Encode(true)
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

	err = i.Close()
	c.Assert(err, IsNil)

	err = b.Close()
	c.Assert(err, IsNil)
}

func (s *MemorySuite) TestDelayed(c *C) {
	b := NewMemoryBroker()
	q, err := b.Queue(bson.NewObjectId().Hex())
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

func (s *MemorySuite) TestTransaction(c *C) {
	b := NewMemoryBroker()
	q, err := b.Queue(bson.NewObjectId().Hex())
	c.Assert(err, IsNil)

	c.Assert(q.Transaction(func(q Queue) error {
		q.Publish(NewJob())
		return fmt.Errorf("err")
	}), IsNil)

	c.Assert(q.(*memoryQueue).jobs, HasLen, 0)

	c.Assert(q.Transaction(func(q Queue) error {
		q.Publish(NewJob())
		q.PublishDelayed(NewJob(), 3*time.Minute)
		return nil
	}), IsNil)

	c.Assert(q.(*memoryQueue).jobs, HasLen, 2)
}
