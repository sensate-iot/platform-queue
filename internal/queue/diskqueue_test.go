package queue

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDiskQueue_Size(t *testing.T) {
	q := NewDiskQueue()

	assert.Equal(t, 0, q.Size())
}
