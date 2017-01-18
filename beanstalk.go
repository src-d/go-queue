package queue

import (
	"errors"
	"fmt"
	"time"

	"github.com/src-d/beanstalk"
)

type beanstalkBroker struct {
	conn *beanstalk.Conn
}

func NewBeanstalkBroker(addr string) (Broker, error) {
	conn, err := beanstalk.Dial(&beanstalk.Config{
		Network: "tcp",
		Addr:    addr,
		Retries: 10,
		Delay:   time.Second * 10,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to beanstalk: %s", err)
	}

	return &beanstalkBroker{conn}, nil
}

func (b *beanstalkBroker) Close() error {
	return b.conn.Close()
}

func (b *beanstalkBroker) Queue(name string) (Queue, error) {
	return &beanstalkQueue{&beanstalk.Tube{Name: name, Conn: b.conn}}, nil
}

type beanstalkQueue struct {
	tube *beanstalk.Tube
}

func (q *beanstalkQueue) Publish(j *Job) error {
	if j == nil || len(j.raw) == 0 {
		return ErrEmptyJob
	}

	var err error
	j.tag, err = q.tube.Put(j.raw, uint32(j.Priority), 0, 1*time.Minute)
	return err
}

func (q *beanstalkQueue) PublishDelayed(j *Job, delay time.Duration) error {
	if j == nil || len(j.raw) == 0 {
		return ErrEmptyJob
	}

	var err error
	j.tag, err = q.tube.Put(j.raw, uint32(j.Priority), delay, 1*time.Minute)
	return err
}

func (q *beanstalkQueue) Transaction(txcb TxCallback) error {
	return errors.New("transactions not supported")
}

func (q *beanstalkQueue) Consume() (JobIter, error) {
	return &beanstalkJobIter{
		t:    beanstalk.NewTubeSet(q.tube.Conn, q.tube.Name),
		name: q.tube.Name,
	}, nil
}

type beanstalkJobIter struct {
	t    *beanstalk.TubeSet
	name string
}

func (i *beanstalkJobIter) Next() (*Job, error) {
	id, body, err := i.t.Reserve(1 * time.Second)
	if err != nil {
		return nil, err
	}

	j := NewJob()
	j.tag = id
	j.ID = fmt.Sprint(id)
	j.raw = body
	j.acknowledger = &beanstalkAcknowledger{
		id:   id,
		conn: i.t.Conn,
	}
	j.contentType = msgpackContentType

	return j, nil
}

func (i *beanstalkJobIter) Close() error { return nil }

type beanstalkAcknowledger struct {
	id   uint64
	conn *beanstalk.Conn
}

func (a *beanstalkAcknowledger) Ack() error {
	return a.conn.Delete(a.id)
}

func (a *beanstalkAcknowledger) Reject(requeue bool) error {
	if !requeue {
		return a.Ack()
	}

	return a.conn.Release(a.id, 1, 0)
}
