package queue

import (
	"io/fs"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type filesByName []fs.FileInfo

// sort.Interface implementation

func (f filesByName) Len() int {
	return len(f)
}

func (f filesByName) Swap(i, j int) {
	f[i], f[j] = f[j], f[i]
}

func (f filesByName) Less(i, j int) bool {
	return f[i].Name() < f[j].Name()
}

func getMinMaxQueueSegment(files []fs.FileInfo) (int,int) {
	if len(files) == 0 {
		return 0, 0
	}

	sort.Sort(filesByName(files))

	firstFile := files[0]
	lastFile  := files[len(files) - 1]

	return fileNameToInteger(firstFile.Name()), fileNameToInteger(lastFile.Name())

}

func fileNameToInteger(fileName string) int {
	name := strings.TrimSuffix(fileName, filepath.Ext(fileName))
	asInt, _ := strconv.Atoi(name)

	return asInt
}
