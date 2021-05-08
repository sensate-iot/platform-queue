package queue

import (
	"fmt"
)

type Deque struct {
	size     int
	capacity int
	offset   int
	values   []interface{}
}

func NewDeque(capacity int) Queue {
	realCapacity := nextPowerOfTwo(capacity)

	q := Deque{
		offset:   0,
		size:     0,
		values:   make([]interface{}, realCapacity),
		capacity: realCapacity,
	}

	return &q
}

func (q *Deque) Enqueue(value interface{}) error {
	if err := q.verifyCapacity(1); err != nil {
		return err
	}

	q.enqueue(value)
	return nil
}

func (q *Deque) EnqueueBatch(values []interface{}) error {
	if err := q.verifyCapacity(len(values)); err != nil {
		return err
	}

	for _, element := range values {
		q.enqueue(element)
	}

	return nil
}

func (q *Deque) Dequeue() (interface{}, error) {
	if err := q.verifySizeForDequeue(); err != nil {
		return nil, err
	}

	rv := q.dequeueBatch(1)

	return rv[0], nil
}

func (q *Deque) DequeueBatch(count int) ([]interface{}, error) {
	if err := q.verifySizeForDequeue(); err != nil {
		return nil, err
	}

	return q.dequeueBatch(count), nil
}

func (q *Deque) Size() int {
	return q.size
}

func (q *Deque) Capacity() int {
	return q.capacity
}

func (q *Deque) Clear() {
	for idx := range q.values {
		q.values[idx] = nil
	}

	q.size = 0
	q.offset = 0
}

func (q *Deque) Close() error {
	q.Clear()
	return nil
}

func (q *Deque) enqueue(value interface{}) {
	idx := q.getArrayIndex(q.size)
	q.values[idx] = value
	q.size += 1
}

func (q *Deque) dequeueBatch(count int) []interface{} {
	dequeueCount := min(count, q.size)
	result := make([]interface{}, dequeueCount)

	if q.isSplit() {
		partition := min(q.capacity-q.offset, dequeueCount)
		remaining := dequeueCount
		q.move(q.offset, q.offset+partition, result)

		if remaining -= partition; remaining > 0 {
			q.move(0, remaining, result[partition:])
		}
	} else {
		q.move(q.offset, q.offset+dequeueCount, result)
	}

	q.offset = (q.offset + count) & (q.capacity - 1)
	q.size -= dequeueCount

	return result
}

func (q *Deque) setCapacity(newCapacity int) error {
	capacity := nextPowerOfTwo(newCapacity)
	slice := make([]interface{}, capacity)

	q.copyInto(slice)
	q.capacity = capacity
	q.offset = 0
	q.values = slice

	return nil
}

func (q *Deque) copyInto(slice []interface{}) {
	if q.isSplit() {
		length := q.capacity - q.offset

		q.move(q.offset, length, slice)
		q.move(0, q.size-length, slice[length-q.offset:])
	} else {
		q.move(q.offset, q.size, slice)
	}
}

func (q *Deque) move(start, end int, dest []interface{}) {
	for idx := range dest {
		valueIndex := start + idx

		if valueIndex >= end {
			break
		}

		dest[idx] = q.values[valueIndex]
		q.values[valueIndex] = nil
	}
}

func (q *Deque) getArrayIndex(idx int) int {
	var index int

	index = idx + q.offset
	index &= q.capacity - 1

	return index
}

func (q *Deque) isSplit() bool {
	return q.offset > q.capacity-q.size
}

func (q *Deque) verifySizeForDequeue() error {
	if q.size <= 0 {
		return fmt.Errorf("deque: unable to dequeue from an empty deque")
	}

	return nil
}

func (q *Deque) verifyCapacity(count int) error {
	newCount := q.size + count

	if newCount <= q.capacity {
		return nil
	}

	return q.setCapacity(newCount)
}

func min(a, b int) int {
	if a < b {
		return a
	}

	return b
}

func nextPowerOfTwo(n int) int {
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n++

	return n
}
