package bookManager

func IsCrossed(book *OrderBook) bool {
	_, _, _, ok := CrossedInfo(book)
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
	if bestBid.PriceTicks < bestAsk.PriceTicks {
		return bestBid, bestAsk, 0, false
	}

	return bestBid, bestAsk, bestBid.PriceTicks - bestAsk.PriceTicks, true
}

type CallbackVerifyChecksum func(bids, asks []Level) uint32
