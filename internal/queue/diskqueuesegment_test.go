package queue

import (
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
)

type segmentTestInterface struct {
	Value string
}

func builderFunc() interface{} {
	return &segmentTestInterface{}
}

func TestDiskQueueSegment_Enqueue(t *testing.T) {
	dir, _ := ioutil.TempDir("", "SegTest")

	seg, err := newDiskQueueSegment(dir, 8, 1, FastMode, builderFunc)

	assert.Nil(t, err)
	err = seg.enqueue(&segmentTestInterface{Value: "abc"})
	assert.Nil(t, err)
}

func TestDiskQueueSegment_EnqueueBatch(t *testing.T) {
	dir, _ := ioutil.TempDir("", "SegTest")
	seg, err := newDiskQueueSegment(dir, 8, 1, FastMode, builderFunc)
	assert.Nil(t, err)

	data := make([]*segmentTestInterface, 10)

	for idx := range data {
		data[idx] = &segmentTestInterface{Value: fmt.Sprintf("Entry %d", idx)}
	}

	count, err := seg.enqueueBatch(segmentTestInterfaceToInterface(data))
	assert.Nil(t, err)
	assert.Equal(t, len(data), count)

	results, err := seg.dequeueBatch(2)
	assert.Nil(t, err)
	values := interfaceToSegmentTestInterface(results)

	assert.Equal(t, "Entry 0", values[0].Value)
}

func TestDiskQueueSegment_Dequeue(t *testing.T) {
	dir, _ := ioutil.TempDir("", "SegTest")

	seg, err := newDiskQueueSegment(dir, 8, 1, FastMode, builderFunc)

	assert.Nil(t, err)
	err = seg.enqueue(&segmentTestInterface{Value: "abc"})
	assert.Nil(t, err)

	obj, err := seg.dequeue()
	item, ok := obj.(*segmentTestInterface)

	assert.True(t, ok)
	assert.Nil(t, err)
	assert.Equal(t, "abc", item.Value)
}

func TestDiskQueueSegment_DequeueBatch(t *testing.T) {
	dir, _ := ioutil.TempDir("", "SegTest")
	seg, err := newDiskQueueSegment(dir, 8, 1, FastMode, builderFunc)
	assert.Nil(t, err)

	_ = seg.enqueue(&segmentTestInterface{Value: "aba"})
	_ = seg.enqueue(&segmentTestInterface{Value: "abb"})
	_ = seg.enqueue(&segmentTestInterface{Value: "abc"})
	_ = seg.enqueue(&segmentTestInterface{Value: "abd"})
	_ = seg.enqueue(&segmentTestInterface{Value: "abe"})

	validateBatchDequeue(t, seg)
}

func TestDiskQueueSegment_CanDeleteSegment(t *testing.T) {
	dir, _ := ioutil.TempDir("", "SegTest")
	seg, err := newDiskQueueSegment(dir, 8, 1, FastMode, builderFunc)
	assert.Nil(t, err)

	_ = seg.enqueue(&segmentTestInterface{Value: "aba"})
	_ = seg.enqueue(&segmentTestInterface{Value: "abb"})
	path := seg.path()

	assert.True(t, fileExists(path))
	err = seg.delete()
	assert.Nil(t, err)
	assert.False(t, fileExists(path))
}

func TestDiskQueueSegment_CannotDeleteNonExistentSegment(t *testing.T) {
	dir, _ := ioutil.TempDir("", "SegTest")
	seg, err := newDiskQueueSegment(dir, 8, 1, FastMode, builderFunc)

	assert.Nil(t, err)
	err = seg.delete()
	assert.Nil(t, err)
	err = seg.delete()
	assert.NotNil(t, err)
}

func TestDiskQueueSegment_Load(t *testing.T) {
	dir, _ := ioutil.TempDir("", "SegTest")

	seg, err := newDiskQueueSegment(dir, 8, 1, FastMode, builderFunc)
	assert.Nil(t, err)

	_ = seg.enqueue(&segmentTestInterface{Value: "aba"})
	_ = seg.enqueue(&segmentTestInterface{Value: "abb"})
	_ = seg.enqueue(&segmentTestInterface{Value: "abc"})
	_ = seg.enqueue(&segmentTestInterface{Value: "abd"})
	_ = seg.enqueue(&segmentTestInterface{Value: "abe"})
	_ = seg.close()

	act, err := loadDiskQueueSegment(dir, 8, 1, FastMode, builderFunc)
	assert.Nil(t, err)

	validateBatchDequeue(t, act)
}

func TestDiskQueueSegment_LoadDeletes(t *testing.T) {
	dir, _ := ioutil.TempDir("", "SegTest")
	seg, err := createSplitSegmentQueue(dir)

	assert.Nil(t, err)
	_ = seg.close()

	act, err := loadDiskQueueSegment(dir, 8, 1, FastMode, builderFunc)
	assert.Nil(t, err)

	objects, err := act.dequeueBatch(3)
	assert.Nil(t, err)

	elements := interfaceToSegmentTestInterface(objects)

	assert.Equal(t, "Entry 3", elements[0].Value)
	assert.Equal(t, "Entry 4", elements[1].Value)
	assert.Equal(t, "Entry 5", elements[2].Value)
}

func TestDiskQueueSegment_CannotLoadFromBadDirectory(t *testing.T) {
	dir, _ := ioutil.TempDir("", "BadDir")
	seg, err := loadDiskQueueSegment(dir, 8, 1, FastMode, builderFunc)

	assert.NotNil(t, err)
	assert.Nil(t, seg)
}

func TestDiskQueueSegment_CannotLoadNonExistentSegment(t *testing.T) {
	dir, _ := ioutil.TempDir("", "SegTest")
	seg, err := createSplitSegmentQueue(dir)

	assert.Nil(t, err)
	_ = seg.close()

	act, err := loadDiskQueueSegment(dir, 8, 2, FastMode, builderFunc)
	assert.Nil(t, act)
	assert.NotNil(t, err)
}

func TestDiskQueueSegment_Size(t *testing.T) {
	dir, _ := ioutil.TempDir("", "SegTest")

	seg, err := newDiskQueueSegment(dir, 8, 1, FastMode, builderFunc)

	assert.Nil(t, err)
	err = seg.enqueue(segmentTestInterface{Value: "abc"})

	assert.Nil(t, err)
	assert.Equal(t, 1, seg.size())
}

func TestDiskQueueSegment_SizeOnDisk(t *testing.T) {
	dir, _ := ioutil.TempDir("", "SegTest")
	q, err := createSplitSegmentQueue(dir)

	assert.Nil(t, err)
	assert.Equal(t, 10, q.sizeOnDisk())
}

func TestDiskQueueSegment_SetMode(t *testing.T) {
	dir, _ := ioutil.TempDir("", "SegTest")
	q, err := newDiskQueueSegment(dir, 8, 1, FastMode, builderFunc)

	assert.Nil(t, err)
	assert.NotNil(t, q)
	assert.Equal(t, FastMode, q.mode)

	q.setMode(FastMode)
	assert.Equal(t, FastMode, q.mode)
}

func TestDiskQueueSegment_NormalMode(t *testing.T) {
	dir, _ := ioutil.TempDir("", "SegTest")
	q, err := newDiskQueueSegment(dir, 8, 1, NormalMode, builderFunc)

	assert.Nil(t, err)
	assert.NotNil(t, q)

	err = q.enqueue(&segmentTestInterface{Value: "Hello 1"})
	assert.Nil(t, err)

	err = q.enqueue(&segmentTestInterface{Value: "Hello 2"})
	assert.Nil(t, err)

	err = q.enqueue(&segmentTestInterface{Value: "Hello 3"})
	assert.Nil(t, err)

	result, err := q.dequeueBatch(2)
	assert.Nil(t, err)
	elements := interfaceToSegmentTestInterface(result)

	assert.Equal(t, "Hello 1", elements[0].Value)
	assert.Equal(t, "Hello 2", elements[1].Value)
}

func validateBatchDequeue(t *testing.T, seg *DiskQueueSegment) {
	objects, err := seg.dequeueBatch(3)
	assert.Nil(t, err)

	elements := interfaceToSegmentTestInterface(objects)

	assert.Equal(t, "aba", elements[0].Value)
	assert.Equal(t, "abb", elements[1].Value)
	assert.Equal(t, "abc", elements[2].Value)
}

func interfaceToSegmentTestInterface(values []interface{}) []segmentTestInterface {
	results := make([]segmentTestInterface, len(values))

	for idx, elem := range values {
		item, _ := elem.(*segmentTestInterface)
		results[idx] = *item
	}

	return results
}

func createSplitSegmentQueue(dir string) (*DiskQueueSegment, error) {
	q, err := newDiskQueueSegment(dir, 8, 1, FastMode, builderFunc)

	if err != nil {
		return nil, err
	}

	for idx := 0; idx < 8; idx++ {
		err := q.enqueue(&segmentTestInterface{Value: fmt.Sprintf("Entry %d", idx)})

		if err != nil {
			return nil, err
		}
	}

	for idx := 0; idx < 3; idx++ {
		_, err := q.dequeue()

		if err != nil {
			return nil, err
		}
	}

	_ = q.enqueue(&segmentTestInterface{Value: "abx"})
	_ = q.enqueue(&segmentTestInterface{Value: "aby"})

	return q, nil
}

func segmentTestInterfaceToInterface(data []*segmentTestInterface) []interface{} {
	result := make([]interface{}, len(data))

	for idx, entry := range data {
		result[idx] = entry
	}

	return result
}
