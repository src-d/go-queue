package memory

import (
	"fmt"
	"io"
	"sync"
	"time"

	queue "gopkg.in/src-d/go-queue.v1"
)

// UnbufferedQueue implements a queue.Queue interface.
type UnbufferedQueue struct {
	jChn               chan *queue.Job
	ackChn             chan bool
	publishImmediately bool
	finite             bool
	sync.Mutex
}

// NewUnbufferedQueue returns an UnbufferedQueue
func NewUnbufferedQueue(publishImmediately bool, finite bool) *UnbufferedQueue {
	return &UnbufferedQueue{
		jChn:               make(chan *queue.Job, 0),
		ackChn:             make(chan bool, 0),
		publishImmediately: publishImmediately,
		finite:             finite,
	}
}

// Publish publishes a Job to the queue.
func (q *UnbufferedQueue) Publish(j *queue.Job) error {
	if j == nil || j.Size() == 0 {
		return queue.ErrEmptyJob.New()
	}

	q.Lock()
	defer q.Unlock()

	acked := false
	for !acked {
		q.jChn <- j
		acked = <-q.ackChn
	}

	return nil
}

// PublishDelayed publishes a Job to the queue with a given delay.
func (q *UnbufferedQueue) PublishDelayed(j *queue.Job, delay time.Duration) error {
	if j == nil || j.Size() == 0 {
		return queue.ErrEmptyJob.New()
	}

	if q.publishImmediately {
		return q.Publish(j)
	}

	go func() {
		time.Sleep(delay)
		q.Publish(j)
	}()
	return nil
}

// RepublishBuried implements the Queue interface.
func (q *UnbufferedQueue) RepublishBuried(conditions ...queue.RepublishConditionFunc) error {
	return fmt.Errorf("RepublishBuried not available for UnbufferedQueue")
}

// Transaction implements the Queue interface.
func (q *UnbufferedQueue) Transaction(txcb queue.TxCallback) error {
	return queue.ErrTxNotSupported.New()
}

// Consume implements the Queue interface. The advertisedWindow value is the
// maximum number of unacknowledged jobs. It is ignored as it is implicitly 0.
func (q *UnbufferedQueue) Consume(advertisedWindow int) (queue.JobIter, error) {
	// advertisedWindow fixed to 0, ignoring provided value
	// log a warning
	return &UnbufferedJobIter{q: q, finite: q.finite}, nil
}

// UnbufferedJobIter implements a queue.JobIter interface.
type UnbufferedJobIter struct {
	q      *UnbufferedQueue
	closed bool
	finite bool
	sync.RWMutex
}

// Next returns the next job in the iter.
func (i *UnbufferedJobIter) Next() (*queue.Job, error) {
	for {
		if i.isClosed() {
			return nil, queue.ErrAlreadyClosed.New()
		}

		select {
		case j := <-i.q.jChn:
			j.Acknowledger = &UnbufferedAcknowledger{q: i.q}
			return j, nil
		default:
			if i.finite {
				return nil, io.EOF
			}
		}

		time.Sleep(1 * time.Second)
	}
}

// Close closes the iter.
func (i *UnbufferedJobIter) Close() error {
	i.Lock()
	defer i.Unlock()
	i.closed = true
	return nil
}

// Close closes the iter.
func (i *UnbufferedJobIter) Release() error {
	return nil
}

func (i *UnbufferedJobIter) isClosed() bool {
	i.RLock()
	defer i.RUnlock()
	return i.closed
}

// UnbufferedAcknowledger implements a queue.Acknowledger interface.
type UnbufferedAcknowledger struct {
	q *UnbufferedQueue
}

// Ack is called when the Job has finished.
func (a *UnbufferedAcknowledger) Ack() error {
	a.q.ackChn <- true
	return nil
}

// Reject is called when the Job has errored. The argument indicates whether
// the Job should be retried by putting it back in the channel.
// If requeue is false, the job will be simply discarded.
func (a *UnbufferedAcknowledger) Reject(requeue bool) error {
	if !requeue {
		return a.Ack()
	}

	a.q.ackChn <- false
	return nil
}
