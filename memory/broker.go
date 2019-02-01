package memory

import (
	"fmt"

	queue "gopkg.in/src-d/go-queue.v1"
)

func init() {
	queue.Register("memory", func(uri string) (queue.Broker, error) {
		return NewBroker(false, false), nil
	})

	queue.Register("memory-finite", func(uri string) (queue.Broker, error) {
		return NewBroker(true, false), nil
	})

	queue.Register("memory-unbuffered", func(uri string) (queue.Broker, error) {
		return NewBroker(false, true), nil
	})

	queue.Register("memory-unbuffered-finite", func(uri string) (queue.Broker, error) {
		return NewBroker(true, true), nil
	})
}

// Broker is a in-memory implementation of Broker.
type Broker struct {
	queues     map[string]queue.Queue
	finite     bool
	unbuffered bool
}

// NewBroker creates a new Broker for an in-memory queue.
func NewBroker(finite bool, unbuffered bool) queue.Broker {
	return &Broker{
		queues:     make(map[string]queue.Queue),
		finite:     finite,
		unbuffered: unbuffered,
	}
}

// Queue returns the queue with the given name.
func (b *Broker) Queue(name string) (queue.Queue, error) {
	if _, ok := b.queues[name]; !ok {
		if !b.unbuffered {
			b.queues[name] = NewQueue(false, b.finite)
		} else {
			// TODO: build UnbufferedQueue
			return nil, fmt.Errorf("error")
		}
	}

	return b.queues[name], nil
}

// Close closes the connection in the Broker.
func (b *Broker) Close() error {
	return nil
}
