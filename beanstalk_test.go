package queue

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"gopkg.in/mgo.v2/bson"
)

func TestBeanstalkSuite(t *testing.T) {
	suite.Run(t, new(BeanstalkSuite))
}

type BeanstalkSuite struct {
	suite.Suite
	broker Broker
}

func (s *BeanstalkSuite) SetupSuite() {
	assert := assert.New(s.T())
	b, err := NewBeanstalkBroker("127.0.0.1:11300")
	assert.NoError(err)
	s.broker = b
}

func (s *BeanstalkSuite) TearDownSuite() {
	assert := assert.New(s.T())
	assert.NoError(s.broker.Close())
}

func (s *BeanstalkSuite) TestPublishAndConsume() {
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
	assert.NoError(retrievedJob.Ack())

	var payload bool
	err = retrievedJob.Decode(&payload)
	assert.NoError(err)
	assert.True(payload)

	assert.Equal(job.tag, retrievedJob.tag)
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

	_, err = i.Next()
	assert.Error(err)

	err = i.Close()
	assert.NoError(err)
}

func (s *BeanstalkSuite) TestDelayed() {
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
