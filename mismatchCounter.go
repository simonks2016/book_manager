package bookManager

import (
	"sync"
	"sync/atomic"
	"time"
)

type MismatchCounter struct {
	mu sync.Mutex

	lastUpdateTime atomic.Int64

	list    map[string]int
	timeMap map[string]time.Time

	expire time.Duration
}

func NewMismatchCounter(expire time.Duration) *MismatchCounter {
	return &MismatchCounter{
		list:    make(map[string]int),
		timeMap: make(map[string]time.Time),
		expire:  expire,
	}
}

func (m *MismatchCounter) Increment(symbol string) int {

	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()

	// 如果太久没更新
	// 说明不是连续 mismatch
	last, ok := m.timeMap[symbol]
	if ok {
		if now.Sub(last) > m.expire {
			m.list[symbol] = 0
		}
	}

	m.list[symbol]++
	m.timeMap[symbol] = now
	m.lastUpdateTime.Store(now.Unix())
	return m.list[symbol]
}

func (m *MismatchCounter) Get(symbol string) int {

	m.mu.Lock()
	defer m.mu.Unlock()

	last, ok := m.timeMap[symbol]
	if !ok {
		return 0
	}

	// 超时断连续
	if time.Since(last) > m.expire {
		delete(m.list, symbol)
		delete(m.timeMap, symbol)
		return 0
	}

	return m.list[symbol]
}

func (m *MismatchCounter) Reset(symbol string) {

	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.list, symbol)
	delete(m.timeMap, symbol)
}

func (m *MismatchCounter) RemoveExpired() {

	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()

	for symbol, t := range m.timeMap {

		if now.Sub(t) > m.expire {
			delete(m.list, symbol)
			delete(m.timeMap, symbol)
		}
	}
}
