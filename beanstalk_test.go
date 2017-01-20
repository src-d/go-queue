package queue

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

func TestBeanstalkSuite(t *testing.T) {
	suite.Run(t, new(BeanstalkSuite))
}

type BeanstalkSuite struct {
	QueueSuite
}

func (s *BeanstalkSuite) SetupSuite() {
	assert := assert.New(s.T())
	b, err := NewBeanstalkBroker("127.0.0.1:11300")
	assert.NoError(err)
	s.Broker = b
	s.TxNotSupported = true
}

func (s *BeanstalkSuite) TearDownSuite() {
	assert := assert.New(s.T())
	assert.NoError(s.Broker.Close())
}
