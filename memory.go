package queue

import (
	"io"
	"sync"
	"time"
)

type memoryBroker struct {
	queues map[string]Queue
}

func NewMemoryBroker() Broker {
	return &memoryBroker{make(map[string]Queue)}
}

func (b *memoryBroker) Queue(name string) (Queue, error) {
	if _, ok := b.queues[name]; !ok {
		b.queues[name] = &memoryQueue{jobs: make([]*Job, 0, 10)}
	}

	return b.queues[name], nil
}

func (b *memoryBroker) Close() error {
	return nil
}

type memoryQueue struct {
	jobs []*Job
	sync.RWMutex
	idx                int
	publishImmediately bool
}

func (q *memoryQueue) Publish(j *Job) error {
	if j == nil || len(j.raw) == 0 {
		return ErrEmptyJob
	}

	q.Lock()
	defer q.Unlock()
	q.jobs = append(q.jobs, j)
	return nil
}

func (q *memoryQueue) PublishDelayed(j *Job, delay time.Duration) error {
	if j == nil || len(j.raw) == 0 {
		return ErrEmptyJob
	}

	if q.publishImmediately {
		return q.Publish(j)
	}
	go func() {
		<-time.After(delay)
		q.Publish(j)
	}()
	return nil
}

func (q *memoryQueue) Transaction(txcb TxCallback) error {
	txQ := &memoryQueue{jobs: make([]*Job, 0, 10), publishImmediately: true}
	if err := txcb(txQ); err != nil {
		return err
	}

	q.jobs = append(q.jobs, txQ.jobs...)
	return nil
}

func (q *memoryQueue) Consume() (JobIter, error) {
	return &memoryJobIter{q: q, RWMutex: &q.RWMutex}, nil
}

type memoryJobIter struct {
	q      *memoryQueue
	closed bool
	*sync.RWMutex
}

type memoryAck struct {
	q *memoryQueue
	j *Job
}

func (*memoryAck) Ack() error {
	return nil
}

func (a *memoryAck) Reject(requeue bool) error {
	if !requeue {
		return nil
	}

	return a.q.Publish(a.j)
}

func (i *memoryJobIter) Next() (*Job, error) {
	for {
		if i.closed {
			return nil, ErrAlreadyClosed
		}

		j, err := i.next()
		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}

		return j, nil
	}
}

func (i *memoryJobIter) next() (*Job, error) {
	i.Lock()
	defer i.Unlock()
	if len(i.q.jobs) <= i.q.idx {
		return nil, io.EOF
	}
	j := i.q.jobs[i.q.idx]
	i.q.idx++
	j.tag = 1
	j.acknowledger = &memoryAck{j: j, q: i.q}
	return j, nil
}

func (i *memoryJobIter) Close() error {
	i.closed = true
	return nil
}
