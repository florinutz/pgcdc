package event

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type Event struct {
	ID        string          `json:"id"`
	Channel   string          `json:"channel"`
	Operation string          `json:"operation"`
	Payload   json.RawMessage `json:"payload"`
	Source    string          `json:"source"`
	CreatedAt time.Time       `json:"created_at"`
}

func New(channel, operation string, payload json.RawMessage, source string) (Event, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return Event{}, fmt.Errorf("generate event id: %w", err)
	}
	return Event{
		ID:        id.String(),
		Channel:   channel,
		Operation: operation,
		Payload:   payload,
		Source:    source,
		CreatedAt: time.Now().UTC(),
	}, nil
}
