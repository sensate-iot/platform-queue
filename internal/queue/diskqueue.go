package queue

import (
	"fmt"
	"github.com/gofrs/flock"
	"io/ioutil"
	"os"
	"path"
	"sync"
)

var (
	MaxUint = ^uint(0)
	MaxInt  = int(MaxUint >> 1)
)

type DiskQueue struct {
	name     string
	basePath string
	builder func() interface{}

	firstSegment *diskQueueSegment
	lastSegmentSequenceNumber int

	lockFile *flock.Flock
	mutex   sync.Mutex
	empty   *sync.Cond

	mode DiskQueueMode
	size int
	segmentCapacity int
}

type DiskQueueMode int

const (
	NormalMode DiskQueueMode = 0
	FastMode   DiskQueueMode = 1
)

func NewDiskQueue(path, name string, builder func() interface{}, segmentCapacity int) (Queue,error) {
	q, err := constructNewDiskQueue(path, name, builder, segmentCapacity)

	if err != nil {
		return nil, err
	}

	q.mutex.Lock()
	defer q.mutex.Unlock()

	if err := q.lock(); err != nil {
		return nil, err
	}

	if err := q.load(); err != nil {
		return nil, err
	}

	return q, nil
}

func (q *DiskQueue) Enqueue(value interface{}) error {
	return nil
}

func (q *DiskQueue) EnqueueBatch(values []interface{}) error {
	return nil
}

func (q *DiskQueue) Dequeue() (interface{}, error) {
	return nil, nil
}

func (q *DiskQueue) DequeueBatch(count int) ([]interface{}, error) {
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

func (q *DiskQueue) load() error {
	fullPath := path.Join(q.basePath, q.name)
	files, err := ioutil.ReadDir(fullPath)

	if err != nil {
		return fmt.Errorf("disk-queue: unable read directory: %v: %v", fullPath, err)
	}

	minSeq, max := getMinMaxQueueSegment(files)

	return q.doLoad(fullPath, minSeq, max)
}

func (q *DiskQueue) doLoad(path string, min, max int) error {
	var err error

	if max > 0 {
		// files found, load data
		err = q.doLoadQueueSegments(path, min, max)
	} else {
		err = q.createNewSegment(path)
	}

	return err
}

func (q *DiskQueue) createNewSegment(path string) error {
	segment, err := newSegment(path, q.segmentCapacity, 1, q.mode, q.builder)

	if err != nil {
		return err
	}

	q.firstSegment = segment
	q.lastSegmentSequenceNumber = segment.sequence

	return nil
}

func (q *DiskQueue) doLoadQueueSegments(path string, min, max int) error {
	segment, err := loadSegment(path, q.segmentCapacity, min, q.mode, q.builder)

	if err != nil {
		return err
	}

	q.firstSegment = segment

	if min == max {
		q.lastSegmentSequenceNumber = min
	} else {
		q.lastSegmentSequenceNumber = max
	}

	return nil
}

func (q* DiskQueue) lock() error {
	lockFile := path.Join(getQueuePath(q), lockFileName(q))
	file := flock.New(lockFile)

	status, err := file.TryLock()

	if err != nil {
		return fmt.Errorf("disk-queue: unable to lock queue '%s': %v", q.name, err)
	}

	if !status {
		return fmt.Errorf("disk-queue: queue '%s' already locked", q.name)
	}

	q.lockFile = file
	return nil
}

func verifyQueue(q *DiskQueue) error {
	if len(q.name) == 0 {
		return fmt.Errorf("disk-queue: queue name length should be greater than 0")
	}

	if len(q.basePath) == 0 {
		return fmt.Errorf("disk-queue: queue base path length should be greater than 0")
	}

	if !dirExists(q.basePath) {
		return fmt.Errorf("disk-queue: queue base path does not exist")
	}

	fullPath := getQueuePath(q)

	if dirExists(fullPath) {
		return fmt.Errorf("disk-queue: queue path already exists (queue might already exist")
	}

	if err := os.Mkdir(fullPath, 0644); err != nil {
		return fmt.Errorf("disk-queue: unable to create queue directory: %v", err)
	}

	return nil
}

func lockFileName(q *DiskQueue) string {
	return fmt.Sprintf("%s.lock", q.name)
}

func getQueuePath(q *DiskQueue) string {
	return path.Join(q.basePath, q.name)
}

func constructNewDiskQueue(path, name string, builder func() interface{}, segmentCapacity int) (*DiskQueue, error) {
	q := DiskQueue{
		name: name,
		basePath: path,
		builder: builder,
		segmentCapacity: segmentCapacity,
		mode: NormalMode,
		size: 0,
	}

	if err := verifyQueue(&q); err != nil {
		return nil, err
	}

	q.empty = sync.NewCond(&q.mutex)

	return &q, nil
}
