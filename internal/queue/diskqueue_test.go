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


func TestDiskQueue_New(t *testing.T) {
	q, err := createDiskQueue()

	assert.Nil(t, err)
	assert.NotNil(t, q)

	diskQ, ok := q.(*DiskQueue)

	assert.True(t, ok)
	assert.Equal(t, diskQ.firstSegment.sequence, 1)
	assert.Equal(t, diskQ.firstSegment.memoryQueue.Capacity(), 128)
	assert.Equal(t, diskQ.lastSegment.sequence, 1)
}

func TestDiskQueue_LoadExisting(t *testing.T) {
	dir, _ := ioutil.TempDir("", "SegTest")
	q1, err := NewDiskQueue(dir, "TestQueue", buildDiskQueueInterface, 100)

	assert.Nil(t, err)
	assert.NotNil(t, q1)

	err = q1.(*DiskQueue).Close()
	q2, err := LoadDiskQueue(dir, "TestQueue", buildDiskQueueInterface, 100)

	assert.Nil(t, err)
	assert.NotNil(t, q2)
}

func TestDiskQueue_Enqueue(t *testing.T) {
	q, _ := createDiskQueue()
	assert.NotNil(t, q)

	err := q.Enqueue(&diskQueueInterface{Value: "Hi"})

	assert.Nil(t, err)
	assert.Equal(t, 1, q.Size())
}

func TestDiskQueue_EnqueueOverflow(t *testing.T) {
	dir, _ := ioutil.TempDir("", "SegTest")
	q, err := NewDiskQueue(dir, "TestQueue", buildDiskQueueInterface, 2)

	assert.Nil(t, err)
	assert.NotNil(t, q)

	err = q.Enqueue(&diskQueueInterface{Value: "Hi 1"})
	assert.Nil(t, err)

	err = q.Enqueue(&diskQueueInterface{Value: "Hi 2"})
	assert.Nil(t, err)

	err = q.Enqueue(&diskQueueInterface{Value: "Hi 3"})

	assert.Nil(t, err)
	assert.Equal(t, 3, q.Size())
}

func TestDiskQueue_EnqueueOverflowTwice(t *testing.T) {
	dir, _ := ioutil.TempDir("", "SegTest")
	q, err := NewDiskQueue(dir, "TestQueue", buildDiskQueueInterface, 2)

	assert.Nil(t, err)
	assert.NotNil(t, q)

	err = q.Enqueue(&diskQueueInterface{Value: "Hi 1"})
	err = q.Enqueue(&diskQueueInterface{Value: "Hi 2"})
	err = q.Enqueue(&diskQueueInterface{Value: "Hi 3"})
	err = q.Enqueue(&diskQueueInterface{Value: "Hi 4"})
	err = q.Enqueue(&diskQueueInterface{Value: "Hi 5"})

	assert.Nil(t, err)
	assert.Equal(t, 5, q.Size())
}

func TestDiskQueue_EnqueueBatch(t *testing.T) {
	dir, _ := ioutil.TempDir("", "SegTest")
	q, err := NewDiskQueue(dir, "TestQueue", buildDiskQueueInterface, 2)
	assert.Nil(t, err)
	assert.NotNil(t, q)

	values := make([]diskQueueInterface, 5)
	values[0] = diskQueueInterface{Value: "Hello 1"}
	values[1] = diskQueueInterface{Value: "Hello 2"}
	values[2] = diskQueueInterface{Value: "Hello 3"}
	values[3] = diskQueueInterface{Value: "Hello 4"}
	values[4] = diskQueueInterface{Value: "Hello 5"}

	err = q.EnqueueBatch(diskQueueInterfaceToInterface(values))

	assert.Nil(t, err)
	assert.Equal(t, 5, q.Size())
	/*result, err := q.Dequeue()

	assert.Nil(t, err)
	assert.Equal(t, 4, q.Size())
	value, ok := result.(diskQueueInterface)
	assert.True(t, ok)
	assert.Equal(t, "Hello 1", value.Value)*/
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

func diskQueueInterfaceToInterface(data []diskQueueInterface) []interface{} {
	result := make([]interface{}, len(data))

	for idx, entry := range data {
		result[idx] = entry
	}

	return result
}

func toDiskQueueInterface(data []interface{}) []diskQueueInterface {
	return nil
}
