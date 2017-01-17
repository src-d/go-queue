package queue

import (
	"errors"
	"time"
)

type Priority uint8

const (
	PriorityUrgent Priority = 8
	PriorityNormal Priority = 4
	PriorityLow    Priority = 0
)

var ErrEmptyJob = errors.New("invalid empty job")

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
	Next() (*Job, error)
	Close() error
}
