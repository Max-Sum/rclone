package accounting

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/rclone/rclone/fs"
)

// transferMap holds a map of strings to Transfer
type transferMap struct {
	mu    sync.RWMutex
	items map[string]*Transfer
	name  string
}

// newTransferMap creates a new empty string set of capacity size
func newTransferMap(size int, name string) *transferMap {
	return &transferMap{
		items: make(map[string]*Transfer, size),
		name:  name,
	}
}

// add adds remote to the set
func (tm *transferMap) add(t *Transfer) {
	tm.mu.Lock()
	tm.items[t.remote] = t
	tm.mu.Unlock()
}

// del removes remote from the set
func (tm *transferMap) del(remote string) {
	tm.mu.Lock()
	delete(tm.items, remote)
	tm.mu.Unlock()
}

// merge adds items from another set
func (tm *transferMap) merge(m *transferMap) {
	tm.mu.Lock()
	m.mu.Lock()
	for r, t := range m.items {
		tm.items[r] = t
	}
	m.mu.Unlock()
	tm.mu.Unlock()
}

// empty returns whether the set has any items
func (tm *transferMap) empty() bool {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return len(tm.items) == 0
}

// count returns the number of items in the set
func (tm *transferMap) count() int {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return len(tm.items)
}

func (tm *transferMap) sortedSlice() []*Transfer {
	s := make([]*Transfer, 0, len(tm.items))
	for _, t := range tm.items {
		s = append(s, t)
	}
	sort.Slice(s, func(i, j int) bool {
		return s[i].startedAt.Before(s[j].startedAt)
	})
	return s
}

// String returns string representation of set items excluding any in
// exclude (if set).
func (tm *transferMap) String(progress *inProgress, exclude *transferMap) string {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	strngs := make([]string, 0, len(tm.items))
	for _, t := range tm.sortedSlice() {
		if exclude != nil {
			exclude.mu.RLock()
			_, found := exclude.items[t.remote]
			exclude.mu.RUnlock()
			if found {
				continue
			}
		}
		var out string
		if acc := progress.get(t.remote); acc != nil {
			out = acc.String()
		} else {
			out = fmt.Sprintf("%*s: %s",
				fs.Config.StatsFileNameLength,
				shortenName(t.remote, fs.Config.StatsFileNameLength),
				tm.name,
			)
		}
		strngs = append(strngs, " * "+out)
	}
	return strings.Join(strngs, "\n")
}

// progress returns total bytes read as well as the size.
func (tm *transferMap) progress(stats *StatsInfo) (totalBytes, totalSize int64) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	for name := range tm.items {
		if acc := stats.inProgress.get(name); acc != nil {
			bytes, size := acc.progress()
			if size >= 0 && bytes >= 0 {
				totalBytes += bytes
				totalSize += size
			}
		}
	}
	return totalBytes, totalSize
}
