# bookManager

`bookManager` is a Go package for maintaining L2 order books with per-symbol FIFO event processing. It is designed for websocket market data streams where snapshots and incremental updates must be applied in the exact order they arrive for the same symbol.

`bookManager` 是一个用于维护 L2 order book 的 Go 包。它的核心目标是：外部只提交 snapshot/update 事件，内部按 symbol 保证 FIFO 串行处理，避免同一交易对的盘口更新被乱序消费后出现 crossed book、checksum mismatch、stale bid/ask 或本地盘口 desync。

## 中文说明

### 核心能力

- 同一个 symbol 的事件严格 FIFO 串行处理。
- 不同 symbol 通过 hash 分区进入不同 worker，可以并发处理。
- `BookManager.Submit(ev BookEvent)` 是外部推荐入口。
- `OrderBook` 只保存和更新盘口状态，不启动 goroutine，不持有 channel。
- `BookManager` 统一负责 worker 分区、dirty 管理、checksum 校验、crossed book 校验和 snapshot 输出过滤。
- 不引入第三方库。

### 处理流程

```text
WS Read
  -> Parse BookEvent
  -> BookManager.Submit(ev)
  -> hash(symbol) % workerCount
  -> FIFO channel
  -> single worker goroutine
  -> ApplySnapshot / ApplyL2Update
  -> Crossed Check / Checksum Verify
  -> MarkDirty / ClearDirty
```

### 模块说明

#### `event.go`

定义事件模型。

- `BookEventType`: 事件类型。
- `EventSnapshot`: snapshot 事件。
- `EventUpdate`: incremental update 事件。
- `BookEvent`: 外部提交给 `BookManager` 的标准事件结构。

字段说明：

- `Symbol`: 交易对或产品 ID，例如 `BTC-USDT`。
- `Type`: `EventSnapshot` 或 `EventUpdate`。
- `Ts`: 事件时间。
- `Levels`: bid/ask levels。
- `Checksum`: 交易所推送的远端 checksum。为 `0` 时只做 crossed book 检查。

#### `bookManager.go`

定义 `BookManager`，是整个包的调度和管理中心。

主要字段：

- `books`: `map[string]*OrderBook`，按 symbol 保存本地盘口。
- `dirty`: `map[string]string`，记录不可信盘口及原因。
- `workers`: `[]chan BookEvent`，分区 worker channel。
- `stopCh`: 停止信号。

主要方法：

- `NewBookManager()`: 使用默认 worker 数和 buffer 创建 manager。
- `NewBookManagerWithWorkers(workerCount, bufferSize)`: 自定义 worker 数和 channel buffer。
- `Submit(ev BookEvent) bool`: 非阻塞提交事件。成功入队返回 `true`，channel 满或 manager 已停止返回 `false`。
- `GetOrCreate(productID string) *OrderBook`: 获取或创建本地盘口。
- `SnapshotTopNAll(n int) []TopNSnapshot`: 获取所有非 dirty symbol 的 TopN snapshot。
- `StartSnapshotTimerAsync(...)`: 定时输出 snapshot。
- `RebuildAllHeaps()`: 重建所有盘口堆。
- `Stop()`: 停止内部 worker。

#### `worker.go`

实现事件消费和顺序处理。

每个 worker channel 只有一个 goroutine 消费，因此 channel 内事件天然 FIFO。因为同一个 symbol 总是通过 `hashSymbol(symbol) % workerCount` 进入同一个 channel，所以同一个 symbol 的 snapshot/update 会按提交顺序串行处理。

worker 处理逻辑：

- snapshot:
  - 必须同时包含 bid 和 ask。
  - 必须满足 `bestBid < bestAsk`。
  - 不合法时标记 dirty，且不 apply。
- update:
  - 允许单边更新。
  - 按顺序调用 `OrderBook.ApplyL2Update`。
- apply 后:
  - 检查 crossed book。
  - 如果 `Checksum > 0`，调用 checksum 校验。
  - 根据结果 `MarkDirty` 或 `ClearDirty`。

#### `dirty.go`

管理不可信盘口。

方法：

- `MarkDirty(symbol, reason)`: 标记 symbol 当前 order book 不可信。
- `ClearDirty(symbol)`: 清除 dirty 状态。
- `IsDirty(symbol) bool`: 查询是否 dirty。

常见 dirty 原因：

- `invalid_snapshot`
- `invalid_update`
- `invalid_event_type`
- `crossed_book`
- `checksum_mismatch`

`SnapshotTopNAll` 会跳过 dirty symbol，避免继续向外输出错误盘口。

#### `validator.go`

提供 crossed book 检查。

- `IsCrossed(book *OrderBook) bool`: 判断是否 `bestBid >= bestAsk`。
- `CrossedInfo(book *OrderBook)`: 返回 best bid、best ask、crossed ticks 和状态。

#### `orderBook.go`

定义纯状态对象 `OrderBook`。

职责：

- 保存 bid/ask map。
- 使用 max heap 维护 bid 最优价。
- 使用 min heap 维护 ask 最优价。
- 应用 snapshot 和 update。
- 输出 TopN snapshot。
- 计算和验证 CRC32 checksum。

主要方法：

- `NewOrderBook(productID string) *OrderBook`
- `ApplySnapshot(ts time.Time, levels ...Level) error`
- `ApplyL2Update(changes []Level, ts time.Time) error`
- `BestBid() (priceTicks int64, size float64, ok bool)`
- `BestAsk() (priceTicks int64, size float64, ok bool)`
- `SnapshotTopN(n int) (bids []Level, asks []Level)`
- `TopNDepth(n int) (bidDepth float64, askDepth float64)`
- `Imbalance(n int) float64`
- `VerifyChecksumByCRC32(remote uint32) bool`
- `ChecksumCRC32() uint32`
- `RebuildHeaps()`

注意：生产场景建议通过 `BookManager.Submit` 更新盘口，不要在 websocket handler 中直接调用 `ApplySnapshot` 或 `ApplyL2Update`，否则会绕过 symbol FIFO、dirty 管理和校验流程。

#### `level.go`

定义盘口价格档。

- `Level.PriceTicks`: 整数 tick 价格。
- `Level.Size`: 数量。
- `Level.IsBids`: `true` 表示 bid，`false` 表示 ask。
- `NewLevel(priceTicks, size, isBids)`: 构造 Level。

#### `default.go`

提供价格和数量转换工具。

- `Scale`: 默认价格缩放比例。
- `NewPrice(px float64) int64`: float 价格转 ticks。
- `PriceTo(priceTicker int64) float64`: ticks 转 float 价格。
- `PriceTicks(priceStr string, tickScale int64) (int64, error)`: 字符串价格转 ticks。
- `SizeFloat(sizeStr string) (float64, error)`: 字符串数量转 float。

#### `maxHeap.go` / `minHeap.go`

内部堆实现。

- `maxHeap`: bid 侧使用，价格越高优先级越高。
- `minHeap`: ask 侧使用，价格越低优先级越高。

这两个模块是内部实现细节，一般业务代码不需要直接使用。

### 使用示例

#### 示例 1：创建 BookManager 并提交 snapshot

```go
package main

import (
	"fmt"
	"time"

	bm "bookManager"
)

func main() {
	manager := bm.NewBookManagerWithWorkers(4, 1024)
	defer manager.Stop()

	ok := manager.Submit(bm.BookEvent{
		Symbol: "BTC-USDT",
		Type:   bm.EventSnapshot,
		Ts:     time.Now(),
		Levels: []bm.Level{
			bm.NewLevel(86400, 1.2, true),
			bm.NewLevel(86410, 0.8, false),
		},
	})
	if !ok {
		fmt.Println("submit failed: worker channel is full")
		return
	}

	time.Sleep(20 * time.Millisecond)

	snapshots := manager.SnapshotTopNAll(1)
	fmt.Printf("%+v\n", snapshots)
}
```

#### 示例 2：提交 L2 update

```go
manager.Submit(bookManager.BookEvent{
	Symbol: "BTC-USDT",
	Type:   bookManager.EventUpdate,
	Ts:     time.Now(),
	Levels: []bookManager.Level{
		bookManager.NewLevel(86405, 0.4, true),  // update bid
		bookManager.NewLevel(86410, 0, false),   // delete ask
		bookManager.NewLevel(86415, 0.7, false), // add ask
	},
})
```

#### 示例 3：接入 websocket handler

```go
ok := p.bookManager.Submit(bookManager.BookEvent{
	Symbol: datum.Symbol,
	Type: func() bookManager.BookEventType {
		if msgType == "snapshot" {
			return bookManager.EventSnapshot
		}
		return bookManager.EventUpdate
	}(),
	Ts:       datum.Timestamp,
	Levels:   levels,
	Checksum: uint32(datum.Checksum),
})

if !ok {
	// channel full: trigger backpressure, reconnect, or resync snapshot
}
```

#### 示例 4：检查 dirty 状态

```go
if manager.IsDirty("BTC-USDT") {
	fmt.Println("BTC-USDT order book is not reliable, wait for resync")
}
```

#### 示例 5：直接使用 OrderBook

只建议在单元测试或离线计算中使用。

```go
book := bookManager.NewOrderBook("BTC-USDT")
_ = book.ApplySnapshot(time.Now(),
	bookManager.NewLevel(10000, 1, true),
	bookManager.NewLevel(10010, 1, false),
)

bids, asks := book.SnapshotTopN(1)
fmt.Println(bids, asks)
```

### BookManager 实现原理

`BookManager` 的关键点是固定分区，而不是普通 worker pool。

普通 worker pool 的问题是：同一个 symbol 的事件可能被不同 goroutine 同时消费。例如 snapshot 被 worker-1 处理，紧接着 update 被 worker-2 处理。如果 update 先完成，就可能造成 stale bid/ask、crossed book 或 checksum mismatch。

`BookManager` 的做法是：

1. 初始化固定数量的 worker channel。
2. `Submit` 根据 symbol 计算 hash。
3. `idx := hashSymbol(symbol) % workerCount`。
4. 同一个 symbol 永远进入同一个 channel。
5. 每个 channel 只有一个 goroutine 消费。
6. Go channel 保证同一个 channel 内 FIFO。
7. 因此同一个 symbol 的事件严格串行，不同 symbol 可以落到不同 channel 并发执行。

这个设计同时满足低延迟和顺序安全：

- `Submit` 不启动单条 goroutine。
- `Submit` 不随机投递到 worker pool。
- 入队是非阻塞的，channel 满时返回 `false`。
- worker 内顺序执行 apply、crossed check、checksum verify 和 dirty 管理。

### Checksum 和 crossed book

当事件带 `Checksum > 0` 时，worker 会在 apply 后调用校验：

- 本地通过 `OrderBook.ChecksumCRC32()` 计算 CRC32。
- 与远端 checksum 比较。
- 不一致则 `MarkDirty(symbol, "checksum_mismatch")`。
- 一致且当前不是 crossed book，则 `ClearDirty(symbol)`。

当事件没有 checksum 时，只做 crossed book 检查：

- `bestBid >= bestAsk` 表示 crossed。
- crossed 时 `MarkDirty(symbol, "crossed_book")`。
- dirty symbol 不会出现在 `SnapshotTopNAll` 输出中。

### 运行测试

```bash
go test ./...
```

## English

### Core Features

- Strict FIFO processing for events of the same symbol.
- Concurrent processing for different symbols through hash-based worker partitioning.
- `BookManager.Submit(ev BookEvent)` is the recommended external write path.
- `OrderBook` remains a pure state object. It does not start goroutines and does not own channels.
- `BookManager` owns worker partitioning, dirty-state management, checksum verification, crossed-book validation, and snapshot filtering.
- No third-party dependencies.

### Processing Pipeline

```text
WS Read
  -> Parse BookEvent
  -> BookManager.Submit(ev)
  -> hash(symbol) % workerCount
  -> FIFO channel
  -> single worker goroutine
  -> ApplySnapshot / ApplyL2Update
  -> Crossed Check / Checksum Verify
  -> MarkDirty / ClearDirty
```

### Module Guide

#### `event.go`

Defines the event model submitted to `BookManager`.

- `BookEventType`: event type.
- `EventSnapshot`: snapshot event.
- `EventUpdate`: incremental update event.
- `BookEvent`: normalized event struct.

Fields:

- `Symbol`: market symbol, such as `BTC-USDT`.
- `Type`: `EventSnapshot` or `EventUpdate`.
- `Ts`: event timestamp.
- `Levels`: bid/ask levels.
- `Checksum`: remote exchange checksum. If it is `0`, only crossed-book validation is performed.

#### `bookManager.go`

Defines `BookManager`, the package-level orchestration component.

Important fields:

- `books`: `map[string]*OrderBook`, local books by symbol.
- `dirty`: `map[string]string`, unreliable books and their reasons.
- `workers`: `[]chan BookEvent`, partitioned event queues.
- `stopCh`: stop signal.

Important methods:

- `NewBookManager()`: creates a manager with default worker count and buffer size.
- `NewBookManagerWithWorkers(workerCount, bufferSize)`: creates a manager with custom worker settings.
- `Submit(ev BookEvent) bool`: non-blocking event submission. Returns `true` when queued successfully, `false` when the queue is full or the manager has stopped.
- `GetOrCreate(productID string) *OrderBook`: gets or creates an order book.
- `SnapshotTopNAll(n int) []TopNSnapshot`: returns TopN snapshots for all non-dirty symbols.
- `StartSnapshotTimerAsync(...)`: periodically emits snapshots.
- `RebuildAllHeaps()`: rebuilds all book heaps.
- `Stop()`: stops internal workers.

#### `worker.go`

Implements event consumption and ordered processing.

Each worker channel has exactly one consumer goroutine. Since a symbol is always mapped to `hashSymbol(symbol) % workerCount`, all events for that symbol go to the same FIFO channel and are applied sequentially.

Worker behavior:

- Snapshot:
  - Must contain both bid and ask levels.
  - Must satisfy `bestBid < bestAsk`.
  - Invalid snapshots are marked dirty and are not applied.
- Update:
  - One-sided updates are allowed.
  - Updates are applied through `OrderBook.ApplyL2Update`.
- After apply:
  - Validate crossed book.
  - Verify checksum if `Checksum > 0`.
  - Mark or clear dirty state.

#### `dirty.go`

Manages unreliable books.

Methods:

- `MarkDirty(symbol, reason)`: marks the local book as unreliable.
- `ClearDirty(symbol)`: clears dirty state.
- `IsDirty(symbol) bool`: checks dirty state.

Common reasons:

- `invalid_snapshot`
- `invalid_update`
- `invalid_event_type`
- `crossed_book`
- `checksum_mismatch`

Dirty symbols are skipped by `SnapshotTopNAll`.

#### `validator.go`

Provides crossed-book validation.

- `IsCrossed(book *OrderBook) bool`: returns true when `bestBid >= bestAsk`.
- `CrossedInfo(book *OrderBook)`: returns best bid, best ask, crossed ticks, and status.

#### `orderBook.go`

Defines the pure order-book state object.

Responsibilities:

- Store bid/ask maps.
- Maintain best bid with a max heap.
- Maintain best ask with a min heap.
- Apply snapshots and updates.
- Generate TopN snapshots.
- Calculate and verify CRC32 checksums.

Important methods:

- `NewOrderBook(productID string) *OrderBook`
- `ApplySnapshot(ts time.Time, levels ...Level) error`
- `ApplyL2Update(changes []Level, ts time.Time) error`
- `BestBid() (priceTicks int64, size float64, ok bool)`
- `BestAsk() (priceTicks int64, size float64, ok bool)`
- `SnapshotTopN(n int) (bids []Level, asks []Level)`
- `TopNDepth(n int) (bidDepth float64, askDepth float64)`
- `Imbalance(n int) float64`
- `VerifyChecksumByCRC32(remote uint32) bool`
- `ChecksumCRC32() uint32`
- `RebuildHeaps()`

In production websocket handlers, prefer `BookManager.Submit` instead of direct calls to `ApplySnapshot` or `ApplyL2Update`; direct calls bypass FIFO ordering, dirty-state management, and validation.

#### `level.go`

Defines one price level.

- `Level.PriceTicks`: integer tick price.
- `Level.Size`: quantity.
- `Level.IsBids`: `true` for bid, `false` for ask.
- `NewLevel(priceTicks, size, isBids)`: creates a level.

#### `default.go`

Provides price and size conversion helpers.

- `Scale`: default price scale.
- `NewPrice(px float64) int64`: converts float price to ticks.
- `PriceTo(priceTicker int64) float64`: converts ticks to float price.
- `PriceTicks(priceStr string, tickScale int64) (int64, error)`: converts string price to ticks.
- `SizeFloat(sizeStr string) (float64, error)`: converts string size to float.

#### `maxHeap.go` / `minHeap.go`

Internal heap implementations.

- `maxHeap`: used for bids; higher price has higher priority.
- `minHeap`: used for asks; lower price has higher priority.

Application code normally does not use these directly.

### Usage Examples

#### Example 1: Create a manager and submit a snapshot

```go
package main

import (
	"fmt"
	"time"

	bm "bookManager"
)

func main() {
	manager := bm.NewBookManagerWithWorkers(4, 1024)
	defer manager.Stop()

	ok := manager.Submit(bm.BookEvent{
		Symbol: "BTC-USDT",
		Type:   bm.EventSnapshot,
		Ts:     time.Now(),
		Levels: []bm.Level{
			bm.NewLevel(86400, 1.2, true),
			bm.NewLevel(86410, 0.8, false),
		},
	})
	if !ok {
		fmt.Println("submit failed: worker channel is full")
		return
	}

	time.Sleep(20 * time.Millisecond)

	snapshots := manager.SnapshotTopNAll(1)
	fmt.Printf("%+v\n", snapshots)
}
```

#### Example 2: Submit an L2 update

```go
manager.Submit(bookManager.BookEvent{
	Symbol: "BTC-USDT",
	Type:   bookManager.EventUpdate,
	Ts:     time.Now(),
	Levels: []bookManager.Level{
		bookManager.NewLevel(86405, 0.4, true),
		bookManager.NewLevel(86410, 0, false),
		bookManager.NewLevel(86415, 0.7, false),
	},
})
```

#### Example 3: Websocket handler integration

```go
ok := p.bookManager.Submit(bookManager.BookEvent{
	Symbol: datum.Symbol,
	Type: func() bookManager.BookEventType {
		if msgType == "snapshot" {
			return bookManager.EventSnapshot
		}
		return bookManager.EventUpdate
	}(),
	Ts:       datum.Timestamp,
	Levels:   levels,
	Checksum: uint32(datum.Checksum),
})

if !ok {
	// channel full: apply backpressure, reconnect, or resync snapshot
}
```

#### Example 4: Check dirty state

```go
if manager.IsDirty("BTC-USDT") {
	fmt.Println("BTC-USDT order book is not reliable; wait for resync")
}
```

#### Example 5: Use OrderBook directly

Recommended only for tests or offline calculations.

```go
book := bookManager.NewOrderBook("BTC-USDT")
_ = book.ApplySnapshot(time.Now(),
	bookManager.NewLevel(10000, 1, true),
	bookManager.NewLevel(10010, 1, false),
)

bids, asks := book.SnapshotTopN(1)
fmt.Println(bids, asks)
```

### How BookManager Works

The key idea is fixed partitioning, not a generic worker pool.

With a normal worker pool, events for the same symbol may be processed by different goroutines. For example, a snapshot may be picked by worker-1 and a following update by worker-2. If the update finishes first, the local book can become stale, crossed, or checksum-invalid.

`BookManager` avoids this by:

1. Creating a fixed number of worker channels.
2. Hashing every symbol on submit.
3. Routing with `idx := hashSymbol(symbol) % workerCount`.
4. Always sending the same symbol to the same channel.
5. Running exactly one goroutine per channel.
6. Relying on Go channel FIFO ordering.
7. Applying events sequentially per symbol while still allowing different symbols to run concurrently.

This provides both low latency and ordering safety:

- `Submit` does not start one goroutine per event.
- `Submit` does not randomly dispatch to a worker pool.
- `Submit` is non-blocking and returns `false` when the target channel is full.
- Each worker applies the event, validates crossed state, verifies checksum, and updates dirty state in order.

### Checksum and Crossed Book

When `Checksum > 0`, the worker verifies the local book after applying the event:

- Local CRC32 is calculated through `OrderBook.ChecksumCRC32()`.
- Local checksum is compared with the remote checksum.
- On mismatch, `MarkDirty(symbol, "checksum_mismatch")` is called.
- On success, dirty state is cleared only when the book is not crossed.

When checksum is `0`, only crossed-book validation is performed:

- `bestBid >= bestAsk` means crossed.
- Crossed books are marked dirty with `crossed_book`.
- Dirty symbols are not returned from `SnapshotTopNAll`.

### Run Tests

```bash
go test ./...
```
