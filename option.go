package bookManager

type Option func(*BookManager)

func WithCrossedThreshold(CrossedThreshold int64) Option {

	return func(m *BookManager) {
		m.CrossedThreshold = CrossedThreshold
	}
}
