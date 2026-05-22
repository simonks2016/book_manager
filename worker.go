package bookManager

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
			if m.onMarkDirty != nil {
				m.onMarkDirty(ev.Symbol, "invalid_snapshot", &ev, book)
			}
			return
		}
		if err := book.ApplySnapshot(ev.Ts, ev.Levels...); err != nil {
			m.MarkDirty(ev.Symbol, "invalid_snapshot")
			if m.onMarkDirty != nil {
				m.onMarkDirty(ev.Symbol, "invalid_snapshot", &ev, book)
			}
			return
		}
	case EventUpdate:
		if err := book.ApplyL2Update(ev.Levels, ev.Ts); err != nil {
			m.MarkDirty(ev.Symbol, "invalid_update")
			if m.onMarkDirty != nil {
				m.onMarkDirty(ev.Symbol, "invalid_update", &ev, book)
			}
			return
		}
	default:
		m.MarkDirty(ev.Symbol, "invalid_event_type")
		if m.onMarkDirty != nil {
			// 标记
			m.onMarkDirty(ev.Symbol, "invalid_event_type", &ev, book)
		}
		return
	}

	crossed := IsCrossed(book)
	if crossed {
		m.MarkDirty(ev.Symbol, "crossed_book")
		if m.onMarkDirty != nil {
			m.onMarkDirty(ev.Symbol, "crossed_book", &ev, book)
		}
	}

	if ev.Checksum > 0 && m.isEnableChecksum {
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

	verified, local := book.VerifyChecksumByCRC32(symbol, checksum)
	if !verified && local > 0 {
		m.MarkDirty(symbol, "checksum_mismatch")
		// 回调函数
		if m.onChecksumFailed != nil {
			m.onChecksumFailed(symbol, local, checksum)
		}
		return false
	}

	if !IsCrossed(book) {
		m.ClearDirty(symbol)
	}
	return true
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
