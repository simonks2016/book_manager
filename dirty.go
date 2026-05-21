package bookManager

func (m *BookManager) MarkDirty(symbol string, reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dirty[symbol] = reason
}

func (m *BookManager) ClearDirty(symbol string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.dirty, symbol)
}

func (m *BookManager) IsDirty(symbol string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.dirty[symbol]
	return ok
}
