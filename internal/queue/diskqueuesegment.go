package queue

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"path"
)

type DiskQueueSegment struct {
	directoryPath string
	sequence      int
	memoryQueue   Queue
	mode          DiskQueueMode
	builder       func() interface{}
	file          *os.File
	removeCount   int
	dirty         bool
}

func newDiskQueueSegment(path string, size, seq int, mode DiskQueueMode, builder func() interface{}) (*DiskQueueSegment, error) {
	seg := DiskQueueSegment{
		memoryQueue:   NewDeque(size),
		directoryPath: path,
		sequence:      seq,
		mode:          mode,
		builder:       builder,
		dirty:         false,
		removeCount:   0,
	}

	if !dirExists(seg.directoryPath) {
		return nil, fmt.Errorf("segment: path is not a valid directory: %v", seg.path())
	}

	if fileExists(seg.path()) {
		return nil, fmt.Errorf("segment: file already exists: %v", seg.path())
	}

	var err error

	seg.file, err = os.OpenFile(seg.path(), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		return nil, fmt.Errorf("segment: unable to create segment file: %v", seg.path())
	}

	return &seg, nil
}

func loadDiskQueueSegment(path string, size, seq int, mode DiskQueueMode, builder func() interface{}) (*DiskQueueSegment, error) {
	seg := DiskQueueSegment{
		memoryQueue:   NewDeque(size),
		directoryPath: path,
		sequence:      seq,
		mode:          mode,
		builder:       builder,
		dirty:         false,
		removeCount:   0,
	}

	if !dirExists(seg.directoryPath) {
		return nil, fmt.Errorf("segment: path is not a valid directory: %v", seg.directoryPath)
	}

	if !fileExists(seg.path()) {
		return nil, fmt.Errorf("segment: segment file for sequence %d does not exists", seg.sequence)
	}

	if err := seg.load(); err != nil {
		return nil, fmt.Errorf("segment: unable to load segment: %v", err)
	}

	var err error
	seg.file, err = os.OpenFile(seg.path(), os.O_APPEND|os.O_WRONLY, 0644)

	if err != nil {
		return nil, fmt.Errorf("segment: unable to reopen segment file: %v", err)
	}

	return &seg, nil
}

func (s *DiskQueueSegment) load() error {
	file, err := os.OpenFile(s.path(), os.O_RDONLY, 0644)

	if err != nil {
		return fmt.Errorf("segment: unable to to open file: %v", err)
	}

	s.file = file
	defer file.Close()

	for {
		done, loadErr := s.doLoad()

		if loadErr != nil {
			return loadErr
		}

		if done {
			break
		}
	}

	return nil
}

func (s *DiskQueueSegment) doLoad() (bool, error) {
	lengthBytes := make([]byte, 4)

	if _, err := io.ReadFull(s.file, lengthBytes); err != nil {
		if err == io.EOF {
			return true, nil
		}

		return false, fmt.Errorf("segment: unable to load segment due to corruption: %d", s.sequence)
	}

	length := binary.LittleEndian.Uint32(lengthBytes)

	if length == 0 {
		if err := s.doLoadRemovalMarker(); err != nil {
			return false, err
		}

		return false, nil
	}

	return false, s.doLoadObject(int(length))
}

func (s *DiskQueueSegment) doLoadRemovalMarker() error {
	if s.memoryQueue.Size() <= 0 {
		return fmt.Errorf("segment: unable to process remove marker on empty queue")
	}

	if _, err := s.memoryQueue.Dequeue(); err != nil {
		return err
	}

	s.removeCount++
	return nil
}

func (s *DiskQueueSegment) doLoadObject(length int) error {
	dataBytes := make([]byte, length)

	if _, err := io.ReadFull(s.file, dataBytes); err != nil {
		return fmt.Errorf("segment: unable to read object from file (%d): %v", s.sequence, err)
	}

	object := s.builder()

	if err := gob.NewDecoder(bytes.NewReader(dataBytes)).Decode(object); err != nil {
		return fmt.Errorf("segment: unable to decode object (segment %d): %v", s.sequence, err)
	}

	return s.memoryQueue.Enqueue(object)
}

func (s *DiskQueueSegment) close() error {
	if err := s.file.Close(); err != nil {
		return fmt.Errorf("segment: unable to close: %v", err)
	}

	return nil
}

func (s *DiskQueueSegment) dequeue() (interface{}, error) {
	if s.memoryQueue.Size() <= 0 {
		return nil, fmt.Errorf("segment: segment is empty")
	}

	return s.doDequeue()
}

func (s *DiskQueueSegment) dequeueBatch(count int) ([]interface{}, error) {
	result, err := s.memoryQueue.DequeueBatch(count)

	if err != nil {
		return nil, err
	}

	for idx := 0; idx < len(result); idx++ {
		dqErr := s.dequeueFile()

		if dqErr != nil {
			return nil, dqErr
		}
	}

	s.removeCount += len(result)
	return result, nil
}

func (s *DiskQueueSegment) dequeueFile() error {
	length := 0
	lengthBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(lengthBytes, uint32(length))

	// Write the delete marker (4-byte zero's)
	if _, err := s.file.Write(lengthBytes); err != nil {
		return fmt.Errorf("segment: unable to write delete marker on "+
			"segment %d: %v", s.sequence, err)
	}

	return nil
}

func (s *DiskQueueSegment) doDequeue() (interface{}, error) {
	if err := s.dequeueFile(); err != nil {
		return nil, err
	}

	object, err := s.memoryQueue.Dequeue()

	if err != nil {
		return nil, fmt.Errorf("segment: unable to deque segment (seq: %d) "+
			"from memory queue: %v", s.sequence, err)
	}

	s.removeCount++

	if err := s.sync(); err != nil {
		return nil, err
	}

	return object, nil
}

func (s *DiskQueueSegment) enqueue(obj interface{}) error {
	if err := s.doEnqueue(obj); err != nil {
		return err
	}

	return s.sync()
}

func (s *DiskQueueSegment) enqueueBatch(objects []interface{}) (int, error) {
	var count int

	for _, entry := range objects {
		if err := s.enqueueToFile(entry); err != nil {
			_ = s.sync()
			return count, err
		}

		count++
	}

	_ = s.memoryQueue.EnqueueBatch(objects)

	return count, s.sync()
}

func (s *DiskQueueSegment) enqueueToFile(obj interface{}) error {
	var buff bytes.Buffer
	enc := gob.NewEncoder(&buff)

	if err := enc.Encode(obj); err != nil {
		return fmt.Errorf("segment: error during gob encoding: %v", err)
	}

	length := len(buff.Bytes())
	lenBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenBytes, uint32(length))

	if _, err := s.file.Write(lenBytes); err != nil {
		return fmt.Errorf("segment: failed to write object length to segment %d", s.sequence)
	}

	if _, err := s.file.Write(buff.Bytes()); err != nil {
		return fmt.Errorf("segment: failed to write gob object to segment %d", s.sequence)
	}

	return nil
}

func (s *DiskQueueSegment) doEnqueue(obj interface{}) error {
	if err := s.enqueueToFile(obj); err != nil {
		return nil
	}

	if s.memoryQueue.Enqueue(obj) != nil {
		return fmt.Errorf("segment: failed to write object of segment %d to the memory queue", s.sequence)
	}

	return nil
}

func (s *DiskQueueSegment) delete() error {
	if err := s.file.Close(); err != nil {
		return fmt.Errorf("segment: unable to close segment file %d: %v", s.sequence, err)
	}

	if err := os.Remove(s.path()); err != nil {
		return fmt.Errorf("segment: unable to delete segment file %d: %v", s.sequence, err)
	}

	s.memoryQueue.Clear()
	s.file = nil

	return nil
}

func (s *DiskQueueSegment) sync() error {
	if s.mode == FastMode {
		s.dirty = true
		return nil
	}

	if err := s.file.Sync(); err != nil {
		return fmt.Errorf("segment: unable sync segment (%d) file to disk", s.sequence)
	}

	s.dirty = false
	return nil
}

func (s *DiskQueueSegment) path() string {
	file := fmt.Sprintf("%016d.que", s.sequence)
	return path.Join(s.directoryPath, file)
}

func (s *DiskQueueSegment) sizeOnDisk() int {
	return s.memoryQueue.Size() + s.removeCount
}

func (s *DiskQueueSegment) empty() bool {
	return s.memoryQueue.Size() == 0
}

func (s *DiskQueueSegment) setMode(mode DiskQueueMode) {
	s.mode = mode
}

func (s *DiskQueueSegment) size() int {
	return s.memoryQueue.Size()
}

func dirExists(path string) bool {
	info, err := os.Stat(path)

	if err == nil {
		return info.IsDir()
	}

	return false
}

func fileExists(path string) bool {
	info, err := os.Stat(path)

	if err == nil {
		return !info.IsDir()
	}

	return false
}
