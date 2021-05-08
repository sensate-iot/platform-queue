package queue

import (
	"fmt"
	"testing"
)

func BenchmarkDeque_Dequeue(b *testing.B) {
	q := createLargeQueue()

	for n := 0; n < b.N; n++ {
		_, _ = q.Dequeue()
	}
}

func BenchmarkDeque_DequeueBatch(b *testing.B) {
	q := createLargeQueue()

	for n := 0; n < b.N; n++ {
		_, _ = q.DequeueBatch(100)
	}
}

func createLargeQueue() Queue {
	q := NewDeque(16000)

	for idx := 0; idx < q.Capacity(); idx++ {
		_ = q.Enqueue(fmt.Sprintf("Entry %d", idx))
	}

	return q
}
