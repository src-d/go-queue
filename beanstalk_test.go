package queue

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

func TestBeanstalkSuite(t *testing.T) {
	suite.Run(t, new(BeanstalkSuite))
}

type BeanstalkSuite struct {
	QueueSuite
}

func (s *BeanstalkSuite) SetupSuite() {
	s.BrokerURI = testBeanstalkURI
	s.TxNotSupported = true
}
