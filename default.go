package bookManager

import (
	"errors"
	"math"
	"strconv"
)

const (
	Scale int = 100
)

func NewPrice(px float64) int64 {
	return int64(math.Round(px * 100))
}

func PriceTo(priceTicker int64) float64 {
	return float64(priceTicker) / 100.00
}

func PriceTicks(priceStr string, tickScale int64) (int64, error) {
	// Use ParseFloat then scale+round. For max robustness, you could parse decimal manually.
	f, err := strconv.ParseFloat(priceStr, 64)
	if err != nil {
		return 0, err
	}
	if f <= 0 {
		return 0, errors.New("price must be > 0")
	}
	v := int64(math.Round(f * float64(tickScale)))
	return v, nil
}

func SizeFloat(sizeStr string) (float64, error) {
	f, err := strconv.ParseFloat(sizeStr, 64)
	if err != nil {
		return 0, err
	}
	// size can be 0 (meaning delete)
	if f < 0 {
		return 0, errors.New("size must be >= 0")
	}
	return f, nil
}
