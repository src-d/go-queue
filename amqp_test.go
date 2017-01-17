package queue

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"gopkg.in/mgo.v2/bson"
)

const amqpURL = "amqp://guest:guest@localhost:5672/"

func TestAMQPSuite(t *testing.T) {
	suite.Run(t, new(AMQPSuite))
}

type AMQPSuite struct {
	suite.Suite
	broker Broker
}

func (s *AMQPSuite) SetupSuite() {
	assert := assert.New(s.T())
	b, err := NewAMQPBroker(amqpURL)
	assert.NoError(err)
	s.broker = b
}

func (s *AMQPSuite) TearDownSuite() {
	assert := assert.New(s.T())
	assert.NoError(s.broker.Close())
}

func (s *AMQPSuite) TestPublishAndConsume() {
	assert := assert.New(s.T())

	q, err := s.broker.Queue(bson.NewObjectId().Hex())
	assert.NoError(err)

	job := NewJob()
	job.Encode(true)
	err = q.Publish(job)
	assert.NoError(err)

	for i := 0; i < 100; i++ {
		job := NewJob()
		job.Encode(i)
		err = q.Publish(job)
		assert.NoError(err)
	}

	i, err := q.Consume()
	assert.NoError(err)

	retrievedJob, err := i.Next()
	assert.NoError(err)
	assert.Nil(retrievedJob.Ack())

	var payload bool
	err = retrievedJob.Decode(&payload)
	assert.NoError(err)
	assert.True(payload)

	assert.Equal(job.ID, retrievedJob.ID)
	assert.Equal(job.Priority, retrievedJob.Priority)
	assert.Equal(job.Timestamp.Second(), retrievedJob.Timestamp.Second())

	for k := 0; k < 100; k++ {
		j, err := i.Next()
		assert.NoError(err)
		if j == nil {
			break
		}

		assert.NoError(j.Ack())
		var payload int
		assert.NoError(j.Decode(&payload))
		assert.Equal(k, payload)
	}

	assert.NoError(i.Close())
}

func (s *AMQPSuite) TestDelayed() {
	assert := assert.New(s.T())

	q, err := s.broker.Queue(bson.NewObjectId().Hex())
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

func (s *AMQPSuite) TestTransaction_Error() {
	assert := assert.New(s.T())

	q, err := s.broker.Queue(bson.NewObjectId().Hex())
	assert.NoError(err)

	err = q.Transaction(func(qu Queue) error {
		job := NewJob()
		assert.NoError(job.Encode("goodbye"))
		assert.NoError(qu.Publish(job))
		return errors.New("foo")
	})
	assert.Error(err)

	i, err := q.Consume()
	assert.NoError(err)
	go func() {
		j, err := i.Next()
		assert.NoError(err)
		assert.Nil(j)
	}()
	<-time.After(50 * time.Millisecond)
	assert.NoError(i.Close())
}

func (s *AMQPSuite) TestTransaction() {
	assert := assert.New(s.T())

	q, err := s.broker.Queue(bson.NewObjectId().Hex())
	assert.NoError(err)

	err = q.Transaction(func(q Queue) error {
		job := NewJob()
		assert.NoError(job.Encode("hello"))
		assert.NoError(q.Publish(job))
		return nil
	})
	assert.NoError(err)

	iter, err := q.Consume()
	assert.NoError(err)
	j, err := iter.Next()
	assert.NoError(err)
	assert.NotNil(j)
	var payload string
	assert.NoError(j.Decode(&payload))
	assert.Equal("hello", payload)
	assert.NoError(iter.Close())
}
