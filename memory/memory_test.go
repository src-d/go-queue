package memory

import (
	"io"
	"testing"

	"gopkg.in/src-d/go-queue.v1"
	"gopkg.in/src-d/go-queue.v1/test"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

func TestMemorySuite(t *testing.T) {
	suite.Run(t, new(MemorySuite))
}

type MemorySuite struct {
	test.QueueSuite
}

func (s *MemorySuite) SetupSuite() {
	s.BrokerURI = "memory://"
}

func (s *MemorySuite) TestIntegration() {
	assert := assert.New(s.T())

	qName := test.NewName()
	q, err := s.Broker.Queue(qName)
	assert.NoError(err)
	assert.NotNil(q)

	j := queue.NewJob()

	j.Encode(true)
	err = q.Publish(j)
	assert.NoError(err)

	for i := 0; i < 100; i++ {
		job := queue.NewJob()

		job.Encode(true)
		err = q.Publish(job)
		assert.NoError(err)
	}

	advertisedWindow := 0 // ignored by memory brokers
	iter, err := q.Consume(advertisedWindow)
	assert.NoError(err)

	retrievedJob, err := iter.Next()
	assert.NoError(err)
	assert.NoError(retrievedJob.Ack())

	var payload bool
	err = retrievedJob.Decode(&payload)
	assert.NoError(err)
	assert.True(payload)

	assert.Equal(j.Priority, retrievedJob.Priority)
	assert.Equal(j.Timestamp.Second(), retrievedJob.Timestamp.Second())

	err = iter.Close()
	assert.NoError(err)
}

func (s *MemorySuite) TestFinite() {
	assert := assert.New(s.T())

	b, err := queue.NewBroker("memoryfinite://")
	assert.NoError(err)

	qName := test.NewName()
	q, err := b.Queue(qName)
	assert.NoError(err)
	assert.NotNil(q)

	j := queue.NewJob()

	j.Encode(true)
	err = q.Publish(j)
	assert.NoError(err)

	advertisedWindow := 0 // ignored by memory brokers
	iter, err := q.Consume(advertisedWindow)
	assert.NoError(err)

	retrievedJob, err := iter.Next()
	assert.NoError(err)
	assert.NoError(retrievedJob.Ack())

	retrievedJob, err = iter.Next()
	assert.Equal(io.EOF, err)
	assert.Nil(retrievedJob)
}
