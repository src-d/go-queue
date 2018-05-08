package queue

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewBroker(t *testing.T) {
	assert := assert.New(t)

	b, err := NewBroker(testAMQPURI)
	assert.NoError(err)
	assert.IsType(&AMQPBroker{}, b)
	assert.NoError(b.Close())

	b, err = NewBroker("amqp://badurl")
	assert.Error(err)

	b, err = NewBroker(testMemoryURI)
	assert.NoError(err)
	assert.IsType(&memoryBroker{}, b)
	assert.NoError(b.Close())

	b, err = NewBroker("memory://anything")
	assert.NoError(err)
	assert.IsType(&memoryBroker{}, b)
	assert.NoError(b.Close())

	b, err = NewBroker("badproto://badurl")
	assert.True(ErrUnsupportedProtocol.Is(err))

	b, err = NewBroker("foo://host%10")
	assert.Error(err)
}
