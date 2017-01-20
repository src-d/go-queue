package queue

import (
	"errors"
	"fmt"
	"time"

	"gopkg.in/mgo.v2/bson"
	"gopkg.in/vmihailenco/msgpack.v2"
)

type contentType string

const msgpackContentType contentType = "application/msgpack"

type Job struct {
	ID        string
	Priority  Priority
	Timestamp time.Time

	contentType  contentType
	raw          []byte
	acknowledger Acknowledger
	tag          uint64
}

type Acknowledger interface {
	Ack() error
	Reject(requeue bool) error
}

// NewJob creates a new Job with default values, a new unique ID and current
// timestamp.
func NewJob() *Job {
	return &Job{
		ID:          bson.NewObjectId().Hex(),
		Priority:    PriorityNormal,
		Timestamp:   time.Now(),
		contentType: msgpackContentType,
	}
}

func (j *Job) Encode(payload interface{}) error {
	var err error
	j.raw, err = encode(msgpackContentType, &payload)
	if err != nil {
		return err
	}

	return nil
}

func (j *Job) Decode(payload interface{}) error {
	return decode(msgpackContentType, j.raw, &payload)
}

var errCantAck = errors.New("can't acknowledge this message, it does not come from a queue")

func (j *Job) Ack() error {
	if j.acknowledger == nil {
		return errCantAck
	}
	return j.acknowledger.Ack()
}

func (j *Job) Reject(requeue bool) error {
	if j.acknowledger == nil {
		return errCantAck
	}
	return j.acknowledger.Reject(requeue)
}

func encode(mime contentType, p interface{}) ([]byte, error) {
	switch mime {
	case msgpackContentType:
		return msgpack.Marshal(p)
	default:
		return nil, fmt.Errorf("unknown content type: %s", mime)
	}
}

func decode(mime contentType, r []byte, p interface{}) error {
	switch mime {
	case msgpackContentType:
		return msgpack.Unmarshal(r, p)
	default:
		return fmt.Errorf("unknown content type: %s", mime)
	}
}
