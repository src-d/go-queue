package queue

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

const amqpURL = "amqp://guest:guest@localhost:5672/"

func TestAMQPSuite(t *testing.T) {
	suite.Run(t, new(AMQPSuite))
}

type AMQPSuite struct {
	QueueSuite
}

func (s *AMQPSuite) SetupTest() {
	assert := assert.New(s.T())
	b, err := NewAMQPBroker(amqpURL)
	assert.NoError(err)
	s.Broker = b
}

func (s *AMQPSuite) TearDownTest() {
	assert := assert.New(s.T())
	assert.NoError(s.Broker.Close())
}

func TestNewAMQPBroker_bad_url(t *testing.T) {
	assert := assert.New(t)

	b, err := NewAMQPBroker("badurl")
	assert.Error(err)
	assert.Nil(b)
}
