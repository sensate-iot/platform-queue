package queue

import (
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"testing"
)

type diskQueueInterface struct {
	Value string
}

func buildDiskQueueInterface() interface{} {
	return &diskQueueInterface{}
}

func TestDiskQueue_CannotCreateWithBadBasePath(t *testing.T) {
	dir := "abcdefg"
	q, err := NewDiskQueue(dir, "TestQueue", buildDiskQueueInterface, 100)

	assert.Nil(t, q)
	assert.NotNil(t, err)
}

func TestDiskQueue_CannotCreateOnExistingDirectory(t *testing.T) {
	dir, _ := ioutil.TempDir("", "SegTest")
	q, err := NewDiskQueue(dir, "TestQueue", buildDiskQueueInterface, 100)

	assert.Nil(t, err)
	assert.NotNil(t, q)

	_, err = NewDiskQueue(dir, "TestQueue", buildDiskQueueInterface, 100)
	assert.NotNil(t, err)
}

func TestDiskQueue_Size(t *testing.T) {
	q,err := createDiskQueue()

	assert.Nil(t, err)
	assert.Equal(t, 0, q.Size())
}

func createDiskQueue() (Queue,error) {
	dir, _ := ioutil.TempDir("", "SegTest")
	q, err := NewDiskQueue(dir, "TestQueue", buildDiskQueueInterface, 100)

	return q,err
}
