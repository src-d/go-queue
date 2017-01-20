package queue

import (
	"errors"
	"io"
	"time"
)

type Priority uint8

const (
	PriorityUrgent Priority = 8
	PriorityNormal Priority = 4
	PriorityLow    Priority = 0
)

var (
	ErrAlreadyClosed  = errors.New("already closed")
	ErrEmptyJob       = errors.New("invalid empty job")
	ErrTxNotSupported = errors.New("transactions not supported")
)

type Broker interface {
	Queue(string) (Queue, error)
	Close() error
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
