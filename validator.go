package bookManager

func IsCrossed(book *OrderBook, CrossedThreshold int64) bool {
	_, _, tick, ok := CrossedInfo(book)

	if !ok {
		return tick >= CrossedThreshold
	}
	return ok
}

func CrossedInfo(book *OrderBook) (
	bestBid Level,
	bestAsk Level,
	crossedTicks int64,
	ok bool,
) {
	if book == nil {
		return Level{}, Level{}, 0, false
	}

	bids, asks := book.SnapshotTopN(1)
	if len(bids) == 0 || len(asks) == 0 {
		return Level{}, Level{}, 0, false
	}

	bestBid = bids[0]
	bestAsk = asks[0]
	if bestBid.PriceTicks <= bestAsk.PriceTicks {
		return bestBid, bestAsk, 0, false
	}

	return bestBid, bestAsk, bestBid.PriceTicks - bestAsk.PriceTicks, true
}

type ChecksumFunc func(symbol string, bids, asks []Level) uint32
type OnChecksumFailed func(symbol string, local uint32, remote uint32)
type OnMarkDirty func(symbol string, reason string, ev *BookEvent, book *OrderBook)
