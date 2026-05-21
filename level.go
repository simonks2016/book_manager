package bookManager

// Level represents one price level for snapshot output (already in ticks & size).
type Level struct {
	PriceTicks int64   `json:"px"`
	Size       float64 `json:"sz"`
	IsBids     bool    `json:"is_bids"`
}

func NewLevel(priceTicks int64, size float64, isBids bool) Level {
	return Level{
		PriceTicks: priceTicks,
		Size:       size,
		IsBids:     isBids,
	}
}
