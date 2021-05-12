package queue

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	q := createQueue(10)

	if q.Capacity() != 16 {
		t.Fatalf("Incorrectly initialized the deque. Expected capacity of %d. Got: %d.", 16, q.Capacity())
	}
}

func TestDeque_CannotDequeueEmptyQueue(t *testing.T) {
	q := createQueue(10)

	result, err := q.Dequeue()

	assert.Nil(t, result)
	assert.NotNil(t, err)
}

func TestDeque_CannotDequeueBatchEmptyQueue(t *testing.T) {
	q := createQueue(10)

	result, err := q.DequeueBatch(5)

	assert.Nil(t, result)
	assert.NotNil(t, err)
}

func TestDeque_Enqueue(t *testing.T) {
	q := createQueue(10)

	if err := q.Enqueue("abc"); err != nil {
		t.Fatal("Unable to enqueue to deque: ", err)
	}

	assert.Equal(t, 1, q.Size())
}

func TestDeque_EnqueueBatch(t *testing.T) {
	q := createQueue(10)
	data := make([]string, 10)

	for idx := range data {
		data[idx] = fmt.Sprintf("Entry %d", idx)
	}

	err := q.EnqueueBatch(toInterface(data))

	assert.Nil(t, err)
	assert.Equal(t, 10, q.Size())

	result, err := q.Dequeue()

	assert.Nil(t, err)
	assert.Equal(t, result, "Entry 0")
}

func TestDeque_EnqueueBatchOverflow(t *testing.T) {
	q := createQueue(4)
	data := make([]string, 10)

	for idx := range data {
		data[idx] = fmt.Sprintf("Entry %d", idx)
	}

	err := q.EnqueueBatch(toInterface(data))

	assert.Nil(t, err)
	assert.Equal(t, 10, q.Size())
	assert.Equal(t, 16, q.Capacity())
}

func TestDeque_EnqueueBatchOverflowSplitQueue(t *testing.T) {
	q, _ := createSplitQueue()
	data := make([]string, 10)

	for idx := range data {
		data[idx] = fmt.Sprintf("Entry %d", idx)
	}

	err := q.EnqueueBatch(toInterface(data))

	assert.Nil(t, err)
	assert.Equal(t, 17, q.Size())
	assert.Equal(t, 32, q.Capacity())
}

func TestDeque_EnqueueOverflow(t *testing.T) {
	q := createQueue(4)
	data := make([]string, 4)

	for idx := range data {
		data[idx] = fmt.Sprintf("Entry %d", idx)
	}

	_ = q.EnqueueBatch(toInterface(data))
	err := q.Enqueue("Fail..")

	assert.Nil(t, err)
	assert.Equal(t, 5, q.Size())
	assert.Equal(t, 8, q.Capacity())
}

func TestDeque_Dequeue(t *testing.T) {
	q := createQueue(10)

	_ = q.Enqueue("abc")
	_ = q.Enqueue("abcd")

	result, err := q.Dequeue()

	if err != nil {
		t.Fatalf("Unable to dequeue: %v", err)
	}

	if result != "abc" {
		t.Fatalf("Got an invalid result after a dequeue: %s", result)
	}
}

func TestDeque_DequeueBatch(t *testing.T) {
	q := createQueue(10)

	_ = q.Enqueue("aba")
	_ = q.Enqueue("abb")
	_ = q.Enqueue("abc")
	_ = q.Enqueue("abd")

	res, err := q.DequeueBatch(3)

	if err != nil {
		t.Fatalf("Unable to dequeue: %v", err)
	}

	assert.Equal(t, 3, len(res))
	assert.Equal(t, "aba", res[0])
	assert.Equal(t, 1, q.Size())
	assert.Equal(t, 16, q.Capacity())
}

func TestDeque_DequeueBatchSplitQueue(t *testing.T) {
	q, err := createSplitQueue()

	if err != nil {
		t.Fatal("Unable to create a split queue: ", err)
	}

	result, err := q.DequeueBatch(6)

	if err != nil {
		t.Fatal("Unable to batch dequeue on a split queue: ", err)
	}

	assert.Equal(t, "Entry 3", result[0])
	assert.Equal(t, "abx", result[5])
	assert.Equal(t, 1, q.Size())
}

func TestDeque_DequeueBatchLargeBatch(t *testing.T) {
	q, err := createSplitQueue()

	if err != nil {
		t.Fatal("Unable to create a split queue: ", err)
	}

	size := q.Size()
	result, err := q.DequeueBatch(100)

	assert.Nil(t, err)
	assert.Equal(t, size, len(result), "Result and original size differ.")
	assert.Equal(t, 0, q.Size())
}

func TestDeque_Clear(t *testing.T) {
	q, _ := createSplitQueue()

	q.Clear()

	assert.Equal(t, q.Size(), 0, "Invalid size!")
	assert.Equal(t, 8, q.Capacity())
}

func TestDeque_Close(t *testing.T) {
	q, _ := createSplitQueue()

	assert.NotNil(t, q)
	assert.Greater(t, q.Size(), 1)

	err := q.Close()

	assert.Nil(t, err)
	assert.Equal(t, 0, q.Size())
}

func createQueue(capacity int) Queue {
	return NewDeque(capacity)
}

func createSplitQueue() (Queue, error) {
	q := NewDeque(8)

	for idx := 0; idx < 8; idx++ {
		err := q.Enqueue(fmt.Sprintf("Entry %d", idx))

		if err != nil {
			return nil, err
		}
	}

	for idx := 0; idx < 3; idx++ {
		_, err := q.Dequeue()

		if err != nil {
			return nil, err
		}
	}

	_ = q.Enqueue("abx")
	_ = q.Enqueue("aby")

	return q, nil
}

func toInterface(data []string) []interface{} {
	result := make([]interface{}, len(data))

	for idx, entry := range data {
		result[idx] = entry
	}

	return result
}
