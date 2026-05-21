package bookManager

import "time"

type BookEventType string

const (
	EventSnapshot BookEventType = "snapshot"
	EventUpdate   BookEventType = "update"
)

type BookEvent struct {
	Symbol   string
	Type     BookEventType
	Ts       time.Time
	Levels   []Level
	Checksum uint32
}
