package queue

var (
	MaxUint = ^uint(0)
	MaxInt  = int(MaxUint >> 1)
)

type DiskQueue struct {
	size int
}

type DiskQueueMode int

const (
	NormalMode DiskQueueMode = 0
	FastMode DiskQueueMode = 1
)

func NewDiskQueue() Queue {
	return &DiskQueue{
		size: 0,
	}
}

func (q *DiskQueue) Enqueue(value interface{}) error {
	return nil
}

func (q *DiskQueue) EnqueueBatch(values []interface{}) error {
	return nil
}

func (q *DiskQueue) Dequeue() (interface{},error) {
	return nil, nil
}

func (q *DiskQueue) DequeueBatch(count int) ([]interface{},error) {
	return nil, nil
}

func (q *DiskQueue) Capacity() int {
	return MaxInt
}

func (q *DiskQueue) Size() int {
	return q.size
}

func (q *DiskQueue) Clear() {

}
