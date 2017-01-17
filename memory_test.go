package queue

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"gopkg.in/mgo.v2/bson"
)

func TestMemorySuite(t *testing.T) {
	suite.Run(t, new(MemorySuite))
}

type MemorySuite struct {
	suite.Suite
}

func (s *MemorySuite) TestIntegration() {
	assert := assert.New(s.T())

	b := NewMemoryBroker()

	q, err := b.Queue(bson.NewObjectId().Hex())
	assert.NoError(err)

	job := NewJob()
	job.Encode(true)
	err = q.Publish(job)
	assert.NoError(err)

	for i := 0; i < 100; i++ {
		job := NewJob()
		job.Encode(true)
		err = q.Publish(job)
		assert.NoError(err)
	}

	i, err := q.Consume()
	assert.NoError(err)

	retrievedJob, err := i.Next()
	assert.NoError(err)
	assert.NoError(retrievedJob.Ack())

	var payload bool
	err = retrievedJob.Decode(&payload)
	assert.NoError(err)
	assert.True(payload)

	assert.Equal(job.tag, retrievedJob.tag)
	assert.Equal(job.Priority, retrievedJob.Priority)
	assert.Equal(job.Timestamp.Second(), retrievedJob.Timestamp.Second())

	err = i.Close()
	assert.NoError(err)

	err = b.Close()
	assert.NoError(err)
}

func (s *MemorySuite) TestDelayed() {
	assert := assert.New(s.T())

	b := NewMemoryBroker()
	q, err := b.Queue(bson.NewObjectId().Hex())
	assert.NoError(err)

	job := NewJob()
	job.Encode("hello")
	err = q.PublishDelayed(job, 1*time.Second)
	assert.NoError(err)

	i, err := q.Consume()
	assert.NoError(err)

	start := time.Now()
	var since time.Duration
	for {
		j, err := i.Next()
		assert.NoError(err)
		if j == nil {
			<-time.After(300 * time.Millisecond)
			continue
		}

		since = time.Since(start)

		var payload string
		assert.NoError(j.Decode(&payload))
		assert.Equal("hello", payload)
		break
	}

	assert.True(since >= 1*time.Second)
}

func (s *MemorySuite) TestTransaction() {
	assert := assert.New(s.T())

	b := NewMemoryBroker()
	q, err := b.Queue(bson.NewObjectId().Hex())
	assert.NoError(err)

	err = q.Transaction(func(q Queue) error {
		q.Publish(NewJob())
		return fmt.Errorf("err")
	})
	assert.NoError(err)

	assert.Len(q.(*memoryQueue).jobs, 0)

	err = q.Transaction(func(q Queue) error {
		q.Publish(NewJob())
		q.PublishDelayed(NewJob(), 3*time.Minute)
		return nil
	})
	assert.NoError(err)
	assert.Len(q.(*memoryQueue).jobs, 2)
}
