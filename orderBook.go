package bookManager

import (
	"container/heap"
	"fmt"
	"hash/crc32"
	"math"
	"strings"
	"sync"
	"time"
)

type OrderBook struct {
	productID string

	mu   sync.RWMutex
	bids map[int64]float64 // priceTicks -> size
	asks map[int64]float64

	bidH maxHeap // store priceTicks (lazy deletion)
	askH minHeap

	lastUpdate time.Time
	callback   ChecksumFunc
}

type OrderBookOption func(*OrderBook)

func NewOrderBook(productID string, opts ...OrderBookOption) *OrderBook {
	ob := &OrderBook{
		productID: productID,
		bids:      make(map[int64]float64, 4096),
		asks:      make(map[int64]float64, 4096),
		bidH:      maxHeap{},
		askH:      minHeap{},
	}
	heap.Init(&ob.bidH)
	heap.Init(&ob.askH)

	for _, opt := range opts {
		opt(ob)
	}

	return ob
}

func (ob *OrderBook) ApplySnapshot(ts time.Time, levels ...Level) error {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	// 1. 使用 Go 1.21+ 的 clear 关键字重置 Map，保留底层内存
	clear(ob.bids)
	clear(ob.asks)

	// 2. 重置 Heap 切片（复用已有内存）
	ob.bidH = ob.bidH[:0]
	ob.askH = ob.askH[:0]

	for _, l := range levels {
		if l.Size <= 0 || l.PriceTicks <= 0 {
			continue
		}

		if l.IsBids {
			ob.bids[l.PriceTicks] = l.Size
			ob.bidH = append(ob.bidH, l.PriceTicks)
		} else {
			ob.asks[l.PriceTicks] = l.Size
			ob.askH = append(ob.askH, l.PriceTicks)
		}
	}

	// 3. 批量建堆，复杂度从 O(N log N) 降到 O(N)
	heap.Init(&ob.bidH)
	heap.Init(&ob.askH)

	ob.lastUpdate = ts
	return nil
}

func (ob *OrderBook) ApplyL2Update(changes []Level, ts time.Time) error {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	for _, ch := range changes {

		px := ch.PriceTicks
		sz := ch.Size

		if ch.IsBids {
			if sz < 1e-6 {
				delete(ob.bids, px)
			} else {
				_, existed := ob.bids[px]
				ob.bids[px] = sz
				if !existed {
					heap.Push(&ob.bidH, px)
				}
			}
		} else {
			if sz < 1e-6 {
				delete(ob.asks, px)
			} else {
				_, exists := ob.asks[px]
				ob.asks[px] = sz
				if !exists {
					heap.Push(&ob.askH, px)
				}
			}
		}
	}

	ob.lastUpdate = ts
	return nil
}

// BestBid returns best bid priceTicks and size, ok=false if empty.
func (ob *OrderBook) BestBid() (priceTicks int64, size float64, ok bool) {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	for {
		px, has := ob.bidH.Peek()
		if !has {
			return 0, 0, false
		}
		sz, exists := ob.bids[px]
		if !exists || sz <= 0 {
			heap.Pop(&ob.bidH) // lazy delete
			continue
		}
		return px, sz, true
	}
}

// BestAsk returns best ask priceTicks and size, ok=false if empty.
func (ob *OrderBook) BestAsk() (priceTicks int64, size float64, ok bool) {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	for {
		px, has := ob.askH.Peek()
		if !has {
			return 0, 0, false
		}
		sz, exists := ob.asks[px]
		if !exists || sz <= 0 {
			heap.Pop(&ob.askH) // lazy delete
			continue
		}
		return px, sz, true
	}
}

// TopNDepth sums sizes of top N levels on each side.
// Note: This scans maps to collect top N; for ultra-high perf, maintain tree/skiplist.
// For many cases (N small like 10/20) and updates batched, this is OK.
func (ob *OrderBook) TopNDepth(n int) (bidDepth float64, askDepth float64) {
	ob.mu.RLock()
	defer ob.mu.RUnlock()

	// collect top N bids: we can scan all keys and keep N best via small heap,
	// but here keep it simple for clarity (still fine when map not huge).
	type kv struct {
		px int64
		sz float64
	}
	bs := make([]kv, 0, len(ob.bids))
	for px, sz := range ob.bids {
		if sz > 0 {
			bs = append(bs, kv{px, sz})
		}
	}
	as := make([]kv, 0, len(ob.asks))
	for px, sz := range ob.asks {
		if sz > 0 {
			as = append(as, kv{px, sz})
		}
	}

	// partial select with sort would be nicer; to stay dependency-free, do O(n*N) selection.
	for i := 0; i < n; i++ {
		// best bid
		best := -1
		var bestPx int64 = math.MinInt64
		for idx := range bs {
			if bs[idx].px > bestPx {
				bestPx = bs[idx].px
				best = idx
			}
		}
		if best >= 0 {
			bidDepth += bs[best].sz
			bs[best].px = math.MinInt64 // mark used
		}

		// best ask
		best = -1
		var bestAskPx int64 = math.MaxInt64
		for idx := range as {
			if as[idx].px < bestAskPx {
				bestAskPx = as[idx].px
				best = idx
			}
		}
		if best >= 0 {
			askDepth += as[best].sz
			as[best].px = math.MaxInt64 // mark used
		}
	}

	return bidDepth, askDepth
}

// Imbalance = (bidDepth - askDepth) / (bidDepth + askDepth)
func (ob *OrderBook) Imbalance(n int) float64 {
	bd, ad := ob.TopNDepth(n)
	den := bd + ad
	if den == 0 {
		return 0
	}
	return (bd - ad) / den
}

// RebuildHeaps 重建Heaps
func (ob *OrderBook) RebuildHeaps() {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	ob.bidH = maxHeap{}
	ob.askH = minHeap{}
	heap.Init(&ob.bidH)
	heap.Init(&ob.askH)

	for px, sz := range ob.bids {
		if sz > 0 {
			heap.Push(&ob.bidH, px)
		}
	}
	for px, sz := range ob.asks {
		if sz > 0 {
			heap.Push(&ob.askH, px)
		}
	}
}

// SnapshotTopN returns TopN bids and asks (best-first).
// It does NOT return full book, only TopN levels on each side.
// Thread-safe.
func (ob *OrderBook) SnapshotTopN(n int) (bids []Level, asks []Level) {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	bids = ob.peekTopNBidsLocked(n)
	asks = ob.peekTopNAsksLocked(n)
	return
}

func (ob *OrderBook) VerifyChecksumByCRC32(symbol string, remote uint32) (bool, uint32) {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	local := ob.checksumCRC32Locked(symbol)
	// 返回结果
	return local == remote, local
}

/*
func (ob *OrderBook) ChecksumCRC32() uint32 {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	return ob.checksumCRC32Locked()
}*/

func (ob *OrderBook) defaultGenChecksum(bids, asks []Level) uint32 {

	var sb strings.Builder

	for _, ask := range asks {
		sb.WriteString(fmt.Sprintf("%d", ask.PriceTicks))
		sb.WriteString(fmt.Sprintf("%.8f", ask.Size))
	}

	for _, bid := range bids {
		sb.WriteString(fmt.Sprintf("%d", bid.PriceTicks))
		sb.WriteString(fmt.Sprintf("%.8f", bid.Size))
	}

	return crc32.ChecksumIEEE([]byte(sb.String()))
}

func (ob *OrderBook) checksumCRC32Locked(symbol string) uint32 {
	bids := ob.peekTopNBidsLocked(10)
	asks := ob.peekTopNAsksLocked(10)

	if ob.callback == nil {
		return ob.defaultGenChecksum(bids, asks)
	} else {
		return ob.callback(symbol, bids, asks)
	}

}

func (ob *OrderBook) peekTopNBidsLocked(n int) []Level {
	if n <= 0 {
		return nil
	}

	out := make([]Level, 0, n)
	popped := make([]int64, 0, n*2)
	seen := make(map[int64]struct{}, n*2)

	for len(out) < n && ob.bidH.Len() > 0 {
		px := heap.Pop(&ob.bidH).(int64)
		popped = append(popped, px)

		if _, ok := seen[px]; ok {
			continue
		}
		seen[px] = struct{}{}

		sz, ok := ob.bids[px]
		if !ok || sz <= 0 {
			continue
		}
		out = append(out, Level{PriceTicks: px, Size: sz, IsBids: true})
	}

	// restore：只恢复有效且不重复的 px（清 stale + 清 heap 重复）
	restored := make(map[int64]struct{}, len(seen))
	for _, px := range popped {
		if _, ok := restored[px]; ok {
			continue
		}
		if sz, ok := ob.bids[px]; ok && sz > 0 {
			heap.Push(&ob.bidH, px)
			restored[px] = struct{}{}
		}
	}

	return out
}

func (ob *OrderBook) peekTopNAsksLocked(n int) []Level {
	if n <= 0 {
		return nil
	}

	out := make([]Level, 0, n)
	popped := make([]int64, 0, n*2)
	seen := make(map[int64]struct{}, n*2)

	for len(out) < n && ob.askH.Len() > 0 {
		px := heap.Pop(&ob.askH).(int64)
		popped = append(popped, px)

		if _, ok := seen[px]; ok {
			continue
		}
		seen[px] = struct{}{}

		sz, ok := ob.asks[px]
		if !ok || sz <= 0 {
			continue
		}
		out = append(out, Level{PriceTicks: px, Size: sz, IsBids: false})
	}

	restored := make(map[int64]struct{}, len(seen))
	for _, px := range popped {
		if _, ok := restored[px]; ok {
			continue
		}
		if sz, ok := ob.asks[px]; ok && sz > 0 {
			heap.Push(&ob.askH, px)
			restored[px] = struct{}{}
		}
	}

	return out
}

func WithChecksum(checksum ChecksumFunc) OrderBookOption {
	return func(ob *OrderBook) {
		if ob.callback == nil {
			return
		}
		ob.callback = checksum
	}
}
