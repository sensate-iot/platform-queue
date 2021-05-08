package queue

type Queue interface {
	Enqueue(interface{}) error
	EnqueueBatch(values []interface{}) error
	Dequeue() (interface{}, error)
	DequeueBatch(count int) ([]interface{}, error)
	Capacity() int
	Size() int
	Clear()
	Close() error
}
