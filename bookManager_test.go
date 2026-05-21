package bookManager

import (
	"testing"
	"time"
)

func TestNewBookManager(t *testing.T) {

	bm := NewBookManagerWithWorkers(4, 100)

	// 提交 snapshot
	ok := bm.Submit(BookEvent{
		Symbol: "BTC-USDT",
		Type:   EventSnapshot,
		Ts:     time.Now(),
		Levels: []Level{
			NewLevel(86400, 0.051, true),
			NewLevel(86410, 1, false),
		},
	})

	if !ok {
		t.Fatal("submit failed")
	}

	// 等 worker 消费
	time.Sleep(100 * time.Millisecond)

	// 获取 snapshot
	snapshots := bm.SnapshotTopNAll(1)

	if len(snapshots) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(snapshots))
	}

	s := snapshots[0]

	if len(s.Bids) != 1 {
		t.Fatal("bid missing")
	}

	if len(s.Asks) != 1 {
		t.Fatal("ask missing")
	}

	if s.Bids[0].PriceTicks != 86400 {
		t.Fatalf("unexpected bid: %d", s.Bids[0].PriceTicks)
	}

	if s.Asks[0].PriceTicks != 86410 {
		t.Fatalf("unexpected ask: %d", s.Asks[0].PriceTicks)
	}

	t.Logf("snapshot=%+v", s)
}
