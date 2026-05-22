package bookManager

import (
	"context"
	"sync"
	"time"
)

type BookManager struct {
	mu       sync.RWMutex
	books    map[string]*OrderBook
	dirty    map[string]string
	workers  []chan BookEvent
	stopCh   chan struct{}
	stopOnce sync.Once

	printDirtyStatus       bool
	isEnableChecksum       bool
	onChecksumFailed       OnChecksumFailed
	callbackVerifyChecksum ChecksumFunc
	onMarkDirty            OnMarkDirty
	mismatchCounter        *MismatchCounter
}

func NewBookManagerWithWorkers(workerCount int, bufferSize int) *BookManager {
	if workerCount <= 0 {
		workerCount = 1
	}
	if bufferSize < 0 {
		bufferSize = 0
	}

	m := &BookManager{
		books:            make(map[string]*OrderBook, 16),
		dirty:            make(map[string]string, 16),
		workers:          make([]chan BookEvent, workerCount),
		stopCh:           make(chan struct{}),
		printDirtyStatus: false,
		isEnableChecksum: true,
		mismatchCounter:  NewMismatchCounter(time.Second * time.Duration(10)),
	}

	for i := 0; i < workerCount; i++ {
		ch := make(chan BookEvent, bufferSize)
		m.workers[i] = ch
		go m.runWorker(ch)
	}

	return m
}

func (m *BookManager) ChecksumFunc(checksum ChecksumFunc) *BookManager {
	m.callbackVerifyChecksum = checksum
	return m
}
func (m *BookManager) EnableChecksum(isEnable bool, failed OnChecksumFailed) *BookManager {
	m.isEnableChecksum = isEnable
	m.onChecksumFailed = failed
	return m
}

func (m *BookManager) OnMarkDirty(fn OnMarkDirty) *BookManager {
	m.onMarkDirty = fn
	return m
}

func (m *BookManager) GetOrCreate(productID string) *OrderBook {
	m.mu.Lock()
	defer m.mu.Unlock()
	if ob, ok := m.books[productID]; ok {
		return ob
	}
	ob := NewOrderBook(productID, WithChecksum(m.callbackVerifyChecksum))
	m.books[productID] = ob
	return ob
}

func (m *BookManager) get(productID string) (*OrderBook, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ob, ok := m.books[productID]
	return ob, ok
}

func (m *BookManager) Stop() {
	m.stopOnce.Do(func() {
		close(m.stopCh)
	})
}

// RebuildAllHeaps 重建Heaps
func (m *BookManager) RebuildAllHeaps() {
	m.mu.RLock()
	books := make([]*OrderBook, 0, len(m.books))
	for _, ob := range m.books {
		books = append(books, ob)
	}
	m.mu.RUnlock()

	for _, ob := range books {
		ob.RebuildHeaps()
	}
}

type TopNSnapshot struct {
	ProductID string  `json:"product_id"`
	Ts        int64   `json:"ts"` // epoch ms
	Bids      []Level `json:"bids"`
	Asks      []Level `json:"asks"`
}

func (m *BookManager) SnapshotTopNAll(n int) []TopNSnapshot {
	m.mu.RLock()
	books := make([]*OrderBook, 0, len(m.books))
	for productID, ob := range m.books {
		if _, ok := m.dirty[productID]; ok {
			continue
		}
		books = append(books, ob)
	}
	m.mu.RUnlock()

	nowMs := time.Now().UnixMilli()
	out := make([]TopNSnapshot, 0, len(books))

	for _, ob := range books {
		bids, asks := ob.SnapshotTopN(n)
		out = append(out, TopNSnapshot{
			ProductID: ob.productID,
			Ts:        nowMs,
			Bids:      bids,
			Asks:      asks,
		})
	}
	return out
}

func (m *BookManager) StartSnapshotTimerAsync(ctx context.Context, interval time.Duration, n int, callback func([]TopNSnapshot)) {

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				snapshots := m.SnapshotTopNAll(n)
				if len(snapshots) == 0 {
					continue
				}
				// 执行回调（建议考虑是否需要 go callback(response) 异步处理）
				go callback(snapshots)
			}
		}
	}()
}

func (m *BookManager) Submit(ev BookEvent) bool {
	if len(m.workers) == 0 {
		return false
	}

	select {
	case <-m.stopCh:
		return false
	default:
	}

	idx := int(hashSymbol(ev.Symbol) % uint32(len(m.workers)))
	select {
	case m.workers[idx] <- ev:
		return true
	default:
		return false
	}
}

func hashSymbol(symbol string) uint32 {
	const (
		offset32 = 2166136261
		prime32  = 16777619
	)

	h := uint32(offset32)
	for i := 0; i < len(symbol); i++ {
		h ^= uint32(symbol[i])
		h *= prime32
	}
	return h
}
