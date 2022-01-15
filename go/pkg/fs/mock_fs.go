package fs

import (
	"fmt"
	pathutil "path"
	"strconv"
	"strings"
	"sync"
	"time"

	"fassst/pkg/utils"
)

type MockKTreeFS struct {
	Depth                uint64
	Degree               uint64
	PageSize             uint64
	MaxCallsPerSecond    uint64
	CallDelayMillis      uint64
	RecentMockCalls      []time.Time
	RecentMockCallsMutex *sync.Mutex
}

type MockKTreePagination struct {
	Depth uint64
	Start uint64
}

func (fs *MockKTreeFS) AddRecentMockCall(time time.Time) bool {
	fs.RecentMockCallsMutex.Lock()
	defer fs.RecentMockCallsMutex.Unlock()

	i := 0
	for i < len(fs.RecentMockCalls) && time.Sub(fs.RecentMockCalls[i]).Seconds() >= 1 {
		i++
	}

	if i > 0 {
		fs.RecentMockCalls = fs.RecentMockCalls[i:]
	}

	if uint64(len(fs.RecentMockCalls)) < fs.MaxCallsPerSecond {
		fs.RecentMockCalls = append(fs.RecentMockCalls, time)
		return true
	}

	return false
}

func (fs *MockKTreeFS) ReadDir(url string, pagination Pagination) ([]DirEntry, []FileEntry, Pagination, error) {
	if !fs.AddRecentMockCall(time.Now()) {
		return nil, nil, nil, fmt.Errorf("too fast; Error Code: SlowDown")
	}
	
	var dirs []DirEntry
	var files []FileEntry
	var start uint64 = 0

	for strings.HasPrefix(url, "/") {
		url = url[1:]
	}

	parts := strings.Split(url, "/")
	depth := uint64(len(parts))

	if depth > fs.Depth {
		return nil, nil, nil, fmt.Errorf("url doesn't exist: %s", url)
	}

	for i := 0; i < len(parts)-1; i++ {
		part := parts[i]
		s, err := strconv.ParseInt(part, 10, 0)
		if err != nil || uint64(s) >= fs.Degree {
			return nil, nil, nil, fmt.Errorf("directory doesn't exist: %s", url)
		}
	}

	if len(parts[len(parts)-1]) > 0 {
		s, err := strconv.ParseInt(parts[len(parts)-1], 10, 0)
		if err != nil || uint64(s) >= fs.Degree {
			return nil, nil, nil, fmt.Errorf("file doesn't exist: %s", url)
		}
		start = uint64(s)
	}

	if pagination != nil {
		p := pagination.(MockKTreePagination)
		depth = p.Depth
		start = p.Start
	}

	if depth > fs.Depth || start >= fs.Degree {
		return nil, nil, nil, fmt.Errorf("pagination doesn't exist: depth=%d start=%d", depth, start)
	}

	base := ""
	if len(parts) > 1 {
		base = strings.Join(parts[:len(parts)-1], "/")
	}

	if depth < fs.Depth {
		for i := start; i < utils.Min(start+fs.PageSize, fs.Degree); i++ {
			dirs = append(dirs, SimpleFileEntry{fmt.Sprintf("%s/%d", base, i)})
		}
	} else {
		for i := start; i < utils.Min(start+fs.PageSize, fs.Degree); i++ {
			files = append(files, SimpleFileEntry{fmt.Sprintf("%s/%d", base, i)})
		}
	}

	if start+fs.PageSize < fs.Degree {
		return dirs, files, MockKTreePagination{depth, start + fs.PageSize}, nil
	}

	time.Sleep(time.Millisecond * time.Duration(fs.CallDelayMillis))

	return dirs, files, nil, nil
}

func (fs MockKTreeFS) ReadFile(path string) ([]byte, error) {
	base := pathutil.Base(path)
	return []byte(base), nil
}

func (fs MockKTreeFS) WriteFile(path string, content []byte) error {
	time.Sleep(time.Microsecond * time.Duration(len(content)))
	return nil
}

func (fs MockKTreeFS) Mkdir(path string) error {
	time.Sleep(time.Microsecond)
	return nil
}
