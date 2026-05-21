package bookManager

import (
	"log"
	"strings"
)

func (m *BookManager) runWorker(ch <-chan BookEvent) {
	for {
		select {
		case <-m.stopCh:
			return
		case ev := <-ch:
			m.handleBookEvent(ev)
		}
	}
}

func (m *BookManager) handleBookEvent(ev BookEvent) {
	if ev.Symbol == "" {
		return
	}

	book := m.GetOrCreate(ev.Symbol)
	switch ev.Type {
	case EventSnapshot:
		if !isValidSnapshotLevels(ev.Levels) {
			m.MarkDirty(ev.Symbol, "invalid_snapshot")
			if m.printDirtyStatus {
				m.logDirty(ev.Symbol, "invalid_snapshot", ev.Type, book, ev.Checksum)
			}
			return
		}
		if err := book.ApplySnapshot(ev.Ts, ev.Levels...); err != nil {
			m.MarkDirty(ev.Symbol, "invalid_snapshot")
			if m.printDirtyStatus {
				m.logDirty(ev.Symbol, "invalid_snapshot", ev.Type, book, ev.Checksum)
			}
			return
		}
	case EventUpdate:
		if err := book.ApplyL2Update(ev.Levels, ev.Ts); err != nil {
			m.MarkDirty(ev.Symbol, "invalid_update")
			if m.printDirtyStatus {
				m.logDirty(ev.Symbol, "invalid_update", ev.Type, book, ev.Checksum)
			}
			return
		}
	default:
		m.MarkDirty(ev.Symbol, "invalid_event_type")
		if m.printDirtyStatus {
			m.logDirty(ev.Symbol, "invalid_event_type", ev.Type, book, ev.Checksum)
		}
		return
	}

	crossed := IsCrossed(book)
	if crossed {
		m.MarkDirty(ev.Symbol, "crossed_book")
		if m.printDirtyStatus {
			m.logDirty(ev.Symbol, "crossed_book", ev.Type, book, ev.Checksum)
		}
	}

	if ev.Checksum > 0 {
		m.verify(ev.Symbol, ev.Checksum, ev.Type)
		return
	}

	if ev.Type == EventSnapshot && !crossed {
		m.ClearDirty(ev.Symbol)
	}
}

func (m *BookManager) Verify(symbol string, checksum uint32) bool {
	return m.verify(symbol, checksum, "")
}

func (m *BookManager) verify(symbol string, checksum uint32, eventType BookEventType) bool {
	book, ok := m.get(symbol)
	if !ok {
		return false
	}

	verified := book.VerifyChecksumByCRC32(checksum)
	if !verified {
		m.MarkDirty(symbol, "checksum_mismatch")

		if m.printDirtyStatus {
			m.logDirty(symbol, "checksum_mismatch", eventType, book, checksum)
		}
		return false
	}

	if !IsCrossed(book) {
		m.ClearDirty(symbol)
	}
	return true
}

func (m *BookManager) logDirty(symbol string, reason string, eventType BookEventType, book *OrderBook, remoteChecksum uint32) {

	if !m.printDirtyStatus {
		return
	}

	bestBid, bestAsk, crossedTicks, _ := CrossedInfo(book)
	localChecksum := uint32(0)
	if book != nil {
		localChecksum = book.ChecksumCRC32()
	}

	label := strings.ReplaceAll(reason, "_", " ")
	log.Printf("[%s %s] reason=%s event=%s bestBid=%+v bestAsk=%+v crossedTicks=%d checksum local=%d remote=%d",
		symbol,
		label,
		reason,
		eventType,
		bestBid,
		bestAsk,
		crossedTicks,
		localChecksum,
		remoteChecksum,
	)
}

func isValidSnapshotLevels(levels []Level) bool {
	hasBid := false
	hasAsk := false
	var bestBid int64
	var bestAsk int64

	for _, level := range levels {
		if level.PriceTicks <= 0 || level.Size <= 0 {
			continue
		}

		if level.IsBids {
			if !hasBid || level.PriceTicks > bestBid {
				bestBid = level.PriceTicks
			}
			hasBid = true
			continue
		}

		if !hasAsk || level.PriceTicks < bestAsk {
			bestAsk = level.PriceTicks
		}
		hasAsk = true
	}

	return hasBid && hasAsk && bestBid < bestAsk
}
