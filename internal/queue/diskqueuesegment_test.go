package queue

import (
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
)

type testInterface struct {
	Value string
}

func builderFunc() interface{} {
	return &testInterface{}
}

func TestDiskQueueSegment_Enqueue(t *testing.T) {
	dir, _ := ioutil.TempDir("", "SegTest")

	seg, err := newSegment(dir, 8, 1, FastMode, builderFunc)

	assert.Nil(t, err)
	err = seg.enqueue(&testInterface{Value: "abc"})
	assert.Nil(t, err)
}

func TestDiskQueueSegment_EnqueueBatch(t *testing.T) {
	dir, _ := ioutil.TempDir("", "SegTest")
	seg, err := newSegment(dir, 8, 1, FastMode, builderFunc)
	assert.Nil(t, err)

	data := make([]*testInterface, 10)

	for idx := range data {
		data[idx] = &testInterface{Value: fmt.Sprintf("Entry %d", idx)}
	}

	err = seg.enqueueBatch(testInterfaceToInterface(data))
	assert.Nil(t, err)

	results, err := seg.dequeueBatch(2)
	assert.Nil(t, err)
	values := toTestInterface(results)

	assert.Equal(t, "Entry 0", values[0].Value)
}

func TestDiskQueueSegment_Dequeue(t *testing.T) {
	dir, _ := ioutil.TempDir("", "SegTest")

	seg, err := newSegment(dir, 8, 1, FastMode, builderFunc)

	assert.Nil(t, err)
	err = seg.enqueue(&testInterface{Value: "abc"})
	assert.Nil(t, err)

	obj, err := seg.dequeue()
	item, ok := obj.(*testInterface)

	assert.True(t, ok)
	assert.Nil(t, err)
	assert.Equal(t, "abc", item.Value)
}

func TestDiskQueueSegment_DequeueBatch(t *testing.T) {
	dir, _ := ioutil.TempDir("", "SegTest")

	seg, err := newSegment(dir, 8, 1, FastMode, builderFunc)
	assert.Nil(t, err)

	_ = seg.enqueue(&testInterface{Value: "aba"})
	_ = seg.enqueue(&testInterface{Value: "abb"})
	_ = seg.enqueue(&testInterface{Value: "abc"})
	_ = seg.enqueue(&testInterface{Value: "abd"})
	_ = seg.enqueue(&testInterface{Value: "abe"})

	validateBatchDequeue(t, seg)
}

func TestDiskQueueSegment_Load(t *testing.T) {
	dir, _ := ioutil.TempDir("", "SegTest")

	seg, err := newSegment(dir, 8, 1, FastMode, builderFunc)
	assert.Nil(t, err)

	_ = seg.enqueue(&testInterface{Value: "aba"})
	_ = seg.enqueue(&testInterface{Value: "abb"})
	_ = seg.enqueue(&testInterface{Value: "abc"})
	_ = seg.enqueue(&testInterface{Value: "abd"})
	_ = seg.enqueue(&testInterface{Value: "abe"})
	_ = seg.close()

	act, err := loadSegment(dir, 8, 1, FastMode, builderFunc)
	assert.Nil(t, err)

	validateBatchDequeue(t, act)
}

func TestDiskQueueSegment_LoadDeletes(t *testing.T) {
	dir, _ := ioutil.TempDir("", "SegTest")
	seg, err := createSplitSegmentQueue(dir)

	assert.Nil(t, err)
	_ = seg.close()

	act, err := loadSegment(dir, 8, 1, FastMode, builderFunc)
	assert.Nil(t, err)

	objects, err := act.dequeueBatch(3)
	assert.Nil(t, err)

	elements := toTestInterface(objects)

	assert.Equal(t, "Entry 3", elements[0].Value)
	assert.Equal(t, "Entry 4", elements[1].Value)
	assert.Equal(t, "Entry 5", elements[2].Value)
}

func TestDiskQueueSegment_CannotLoadFromBadDirectory(t *testing.T) {
	dir, _ := ioutil.TempDir("", "BadDir")
	seg, err := loadSegment(dir, 8, 1, FastMode, builderFunc)

	assert.NotNil(t, err)
	assert.Nil(t, seg)
}

func TestDiskQueueSegment_CannotLoadNonExistentSegment(t *testing.T) {
	dir, _ := ioutil.TempDir("", "SegTest")
	seg, err := createSplitSegmentQueue(dir)

	assert.Nil(t, err)
	_ = seg.close()

	act, err := loadSegment(dir, 8, 2, FastMode, builderFunc)
	assert.Nil(t, act)
	assert.NotNil(t, err)
}

func TestDiskQueueSegment_Size(t *testing.T) {
	dir, _ := ioutil.TempDir("", "SegTest")

	seg, err := newSegment(dir, 8, 1, FastMode, builderFunc)

	assert.Nil(t, err)
	err = seg.enqueue(testInterface{Value: "abc"})

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
	q, err := newSegment(dir, 8, 1, FastMode, builderFunc)

	assert.Nil(t, err)
	assert.NotNil(t, q)
	assert.Equal(t, FastMode, q.mode)

	q.setMode(FastMode)
	assert.Equal(t, FastMode, q.mode)
}

func TestDiskQueueSegment_NormalMode(t *testing.T) {
	dir, _ := ioutil.TempDir("", "SegTest")
	q, err := newSegment(dir, 8, 1, NormalMode, builderFunc)

	assert.Nil(t, err)
	assert.NotNil(t, q)

	err = q.enqueue(&testInterface{Value: "Hello 1"})
	assert.Nil(t, err)

	err = q.enqueue(&testInterface{Value: "Hello 2"})
	assert.Nil(t, err)

	err = q.enqueue(&testInterface{Value: "Hello 3"})
	assert.Nil(t, err)

	result, err := q.dequeueBatch(2)
	assert.Nil(t, err)
	elements := toTestInterface(result)

	assert.Equal(t, "Hello 1", elements[0].Value)
	assert.Equal(t, "Hello 2", elements[1].Value)
}

func validateBatchDequeue(t *testing.T, seg *diskQueueSegment) {
	objects, err := seg.dequeueBatch(3)
	assert.Nil(t, err)

	elements := toTestInterface(objects)

	assert.Equal(t, "aba", elements[0].Value)
	assert.Equal(t, "abb", elements[1].Value)
	assert.Equal(t, "abc", elements[2].Value)
}

func toTestInterface(values []interface{}) []testInterface {
	results := make([]testInterface, len(values))

	for idx, elem := range values {
		item, _ := elem.(*testInterface)
		results[idx] = *item
	}

	return results
}

func createSplitSegmentQueue(dir string) (*diskQueueSegment, error) {
	q, err := newSegment(dir, 8, 1, FastMode, builderFunc)

	if err != nil {
		return nil, err
	}

	for idx := 0; idx < 8; idx++ {
		err := q.enqueue(&testInterface{Value: fmt.Sprintf("Entry %d", idx)})

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

	_ = q.enqueue(&testInterface{Value: "abx"})
	_ = q.enqueue(&testInterface{Value: "aby"})

	return q, nil
}

func testInterfaceToInterface(data []*testInterface) []interface{} {
	result := make([]interface{}, len(data))

	for idx, entry := range data {
		result[idx] = entry
	}

	return result
}
