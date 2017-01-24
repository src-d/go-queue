package queue

import (
	"errors"
	"io"
	"net/url"
	"time"
)

type Priority uint8

const (
	PriorityUrgent Priority = 8
	PriorityNormal Priority = 4
	PriorityLow    Priority = 0
)

var (
	ErrAlreadyClosed       = errors.New("already closed")
	ErrEmptyJob            = errors.New("invalid empty job")
	ErrTxNotSupported      = errors.New("transactions not supported")
	ErrUnsupportedProtocol = errors.New("unsupported protocol")
)

const (
	protoAMQP      string = "amqp"
	protoBeanstalk        = "beanstalk"
	protoMemory           = "memory"
)

type Broker interface {
	Queue(string) (Queue, error)
	Close() error
}

// NewBroker creates a new Broker based on the given URI. Possible URIs are
//   amqp://<host>[:port]
//   beanstalk://<host>[:port]
//   memory://
func NewBroker(uri string) (Broker, error) {
	url, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}

	switch url.Scheme {
	case protoAMQP:
		return NewAMQPBroker(uri)
	case protoBeanstalk:
		return NewBeanstalkBroker(uri[len(protoBeanstalk)+3:])
	case protoMemory:
		return NewMemoryBroker(), nil
	default:
		return nil, ErrUnsupportedProtocol
	}
}

type TxCallback func(q Queue) error

type Queue interface {
	Publish(*Job) error
	PublishDelayed(*Job, time.Duration) error
	Transaction(TxCallback) error
	Consume() (JobIter, error)
}

type JobIter interface {
	// Next returns the next Job in the iterator. It should block until the
	// job becomes available. Returns ErrAlreadyClosed if the iterator is
	// closed.
	Next() (*Job, error)
	io.Closer
}
