package bookManager

func (m *BookManager) MarkDirty(symbol string, reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.dirty[symbol] = reason
}

func (m *BookManager) ClearDirty(symbol string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 清理dirty的时候
	delete(m.dirty, symbol)
	//
	m.mismatchCounter.Reset(symbol)
}

func (m *BookManager) IsDirty(symbol string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.dirty[symbol]
	return ok
}
