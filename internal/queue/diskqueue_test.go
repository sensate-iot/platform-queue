package queue

import (
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
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
	assert.Nil(t, err)
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

	_ = q.Enqueue(&diskQueueInterface{Value: "Hi 1"})
	_ = q.Enqueue(&diskQueueInterface{Value: "Hi 2"})
	_ = q.Enqueue(&diskQueueInterface{Value: "Hi 3"})
	_ = q.Enqueue(&diskQueueInterface{Value: "Hi 4"})
	err = q.Enqueue(&diskQueueInterface{Value: "Hi 5"})

	assert.Nil(t, err)
	assert.Equal(t, 5, q.Size())
}

func TestDiskQueue_EnqueueBatch(t *testing.T) {
	q, err := createMultiQueue()

	assert.Nil(t, err)
	assert.Equal(t, 5, q.Size())
	result, err := q.Dequeue()

	assert.Nil(t, err)
	assert.Equal(t, 4, q.Size())
	value, ok := result.(*diskQueueInterface)
	assert.True(t, ok)
	assert.Equal(t, "Hello 1", value.Value)
}

func TestDiskQueue_DequeueBatchEmpty(t *testing.T) {
	q, err := createDiskQueue()

	assert.Nil(t, err)
	assert.NotNil(t, q)

	result, err := q.DequeueBatch(100)
	assert.NotNil(t, err)
	assert.Nil(t, result)
}

func TestDiskQueue_DequeueOverflow(t *testing.T) {
	q, err := createMultiQueue()
	assert.Nil(t, err)
	assert.NotNil(t, q)
	_ = q.Enqueue(&diskQueueInterface{Value: "Hello 6"})

	result, err := q.Dequeue()
	assert.Nil(t, err)
	value, ok := result.(*diskQueueInterface)
	assert.True(t, ok)
	assert.Equal(t, "Hello 1", value.Value)

	result, err = q.Dequeue()
	assert.Nil(t, err)
	value, ok = result.(*diskQueueInterface)
	assert.True(t, ok)
	assert.Equal(t, "Hello 2", value.Value)

	result, err = q.Dequeue()
	assert.Nil(t, err)
	value, ok = result.(*diskQueueInterface)
	assert.True(t, ok)
	assert.Equal(t, "Hello 3", value.Value)

	result, err = q.Dequeue()
	assert.Nil(t, err)
	value, ok = result.(*diskQueueInterface)
	assert.True(t, ok)
	assert.Equal(t, "Hello 4", value.Value)

	result, err = q.Dequeue()
	assert.Nil(t, err)
	value, ok = result.(*diskQueueInterface)
	assert.True(t, ok)
	assert.Equal(t, "Hello 5", value.Value)

	result, err = q.Dequeue()
	assert.Nil(t, err)
	value, ok = result.(*diskQueueInterface)
	assert.True(t, ok)
	assert.Equal(t, "Hello 6", value.Value)
}

func TestDiskQueue_Size(t *testing.T) {
	q, err := createDiskQueue()

	assert.Nil(t, err)
	assert.Equal(t, 0, q.Size())
}

func TestDiskQueue_Capacity(t *testing.T) {
	q, err := createDiskQueue()

	assert.Nil(t, err)
	assert.Equal(t, MaxInt, q.Capacity())
}

func TestDiskQueue_DequeueBatch(t *testing.T) {
	q, err := createMultiQueue()
	assert.Nil(t, err)

	batch, err := q.DequeueBatch(4)
	assert.Nil(t, err)
	assert.Equal(t, 4, len(batch))

	values := toDiskQueueInterface(batch)

	assert.Equal(t, "Hello 1", values[0].Value)
	assert.Equal(t, "Hello 4", values[3].Value)
}

func TestDiskQueue_Close(t *testing.T) {
	q, err := createMultiQueue()

	assert.Nil(t, err)
	assert.NotNil(t, q)

	err = q.Close()
	assert.Nil(t, err)
}

func TestDiskQueue_DequeueBatchUnderflow(t *testing.T) {
	q, err := createMultiQueue()
	assert.Nil(t, err)

	batch, err := q.DequeueBatch(100)
	assert.Nil(t, err)
	assert.Equal(t, 5, len(batch))

	values := toDiskQueueInterface(batch)

	assert.Equal(t, "Hello 1", values[0].Value)
	assert.Equal(t, "Hello 4", values[3].Value)
	assert.Equal(t, "Hello 5", values[4].Value)
}

func createDiskQueue() (Queue, error) {
	dir, _ := ioutil.TempDir("", "SegTest")
	q, err := NewDiskQueue(dir, "TestQueue", buildDiskQueueInterface, 100)

	return q, err
}

func createMultiQueue() (Queue, error) {
	dir, _ := ioutil.TempDir("", "SegTest")
	q, err := NewDiskQueue(dir, "TestQueue", buildDiskQueueInterface, 2)

	if err != nil {
		return nil, err
	}

	values := make([]*diskQueueInterface, 5)
	values[0] = &diskQueueInterface{Value: "Hello 1"}
	values[1] = &diskQueueInterface{Value: "Hello 2"}
	values[2] = &diskQueueInterface{Value: "Hello 3"}
	values[3] = &diskQueueInterface{Value: "Hello 4"}
	values[4] = &diskQueueInterface{Value: "Hello 5"}

	err = q.EnqueueBatch(diskQueueInterfaceToInterface(values))
	return q, err
}

func diskQueueInterfaceToInterface(data []*diskQueueInterface) []interface{} {
	result := make([]interface{}, len(data))

	for idx, entry := range data {
		result[idx] = entry
	}

	return result
}

func toDiskQueueInterface(data []interface{}) []diskQueueInterface {
	results := make([]diskQueueInterface, len(data))

	for idx, elem := range data {
		item, _ := elem.(*diskQueueInterface)
		results[idx] = *item
	}

	return results
}
