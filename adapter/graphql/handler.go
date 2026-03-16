package graphql

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/coder/websocket"
	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/metrics"
)

// subscription represents an active client subscription.
type subscription struct {
	id         string
	filter     *Filter
	selections []string // field projections (nil = all)
	ch         chan event.Event
}

// client represents a connected WebSocket client with its subscriptions.
type client struct {
	conn   *websocket.Conn
	ctx    context.Context
	cancel context.CancelFunc
	subs   map[string]*subscription // id -> subscription
	logger *slog.Logger
}

// ServeHTTP upgrades the connection to WebSocket and runs the
// graphql-transport-ws protocol.
func (a *Adapter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		Subprotocols: []string{Subprotocol},
	})
	if err != nil {
		a.logger.Warn("websocket accept failed", "error", err)
		return
	}

	if conn.Subprotocol() != Subprotocol {
		_ = conn.Close(websocket.StatusPolicyViolation, "subprotocol must be "+Subprotocol)
		return
	}

	ctx, cancel := context.WithCancel(r.Context())
	c := &client{
		conn:   conn,
		ctx:    ctx,
		cancel: cancel,
		subs:   make(map[string]*subscription),
		logger: a.logger,
	}

	metrics.GraphQLClientsActive.Inc()
	defer func() {
		metrics.GraphQLClientsActive.Dec()
		c.cancel()
		a.removeClient(c)
		_ = conn.CloseNow()
	}()

	// Wait for connection_init with a timeout.
	if err := a.handleConnectionInit(c); err != nil {
		a.logger.Debug("connection init failed", "error", err)
		return
	}

	// Start keepalive ping ticker.
	pingDone := make(chan struct{})
	go func() {
		defer close(pingDone)
		a.runKeepalive(c)
	}()

	// Read loop: handle incoming messages.
	a.readLoop(c)

	<-pingDone
}

// SchemaHandler serves the generated GraphQL schema as plain text.
func (a *Adapter) SchemaHandler(w http.ResponseWriter, r *http.Request) {
	schemaStr := GenerateSchema(a.schemaStore)
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write([]byte(schemaStr))
}

// handleConnectionInit waits for a connection_init message and responds with connection_ack.
func (a *Adapter) handleConnectionInit(c *client) error {
	initCtx, initCancel := context.WithTimeout(c.ctx, 15*time.Second)
	defer initCancel()

	_, data, err := c.conn.Read(initCtx)
	if err != nil {
		return fmt.Errorf("read connection_init: %w", err)
	}

	var msg Message
	if err := json.Unmarshal(data, &msg); err != nil {
		return fmt.Errorf("parse connection_init: %w", err)
	}
	if msg.Type != MsgConnectionInit {
		_ = c.conn.Close(4429, "too many initialization requests")
		return fmt.Errorf("expected connection_init, got %s", msg.Type)
	}

	ack := Message{Type: MsgConnectionAck}
	ackData, _ := json.Marshal(ack)
	if err := c.conn.Write(c.ctx, websocket.MessageText, ackData); err != nil {
		return fmt.Errorf("write connection_ack: %w", err)
	}

	return nil
}

// readLoop reads and dispatches incoming WebSocket messages.
func (a *Adapter) readLoop(c *client) {
	for {
		_, data, err := c.conn.Read(c.ctx)
		if err != nil {
			return
		}

		var msg Message
		if err := json.Unmarshal(data, &msg); err != nil {
			a.logger.Debug("invalid message", "error", err)
			continue
		}

		switch msg.Type {
		case MsgSubscribe:
			a.handleSubscribe(c, msg)
		case MsgComplete:
			a.handleComplete(c, msg)
		case MsgPing:
			a.handlePing(c)
		default:
			a.logger.Debug("unhandled message type", "type", msg.Type)
		}
	}
}

// handleSubscribe processes a subscribe message and starts streaming matching events.
func (a *Adapter) handleSubscribe(c *client, msg Message) {
	if msg.ID == "" {
		a.sendError(c, "", "subscribe message must have an id")
		return
	}
	if _, exists := c.subs[msg.ID]; exists {
		_ = c.conn.Close(4409, fmt.Sprintf("subscriber for %s already exists", msg.ID))
		return
	}

	var payload SubscribePayload
	if err := json.Unmarshal(msg.Payload, &payload); err != nil {
		a.sendError(c, msg.ID, fmt.Sprintf("invalid subscribe payload: %v", err))
		return
	}

	_, selections, variables := parseSubscription(payload)
	filter := ParseFilter(variables)

	sub := &subscription{
		id:         msg.ID,
		filter:     filter,
		selections: selections,
		ch:         make(chan event.Event, a.bufferSize),
	}

	if !a.addSubscription(c, sub) {
		close(sub.ch)
		a.sendError(c, msg.ID, "max clients reached")
		return
	}
	c.subs[msg.ID] = sub

	metrics.GraphQLSubscriptionsActive.Inc()

	// Start goroutine to send events for this subscription.
	go a.streamSubscription(c, sub)
}

// handleComplete processes a complete message, removing the subscription.
func (a *Adapter) handleComplete(c *client, msg Message) {
	sub, exists := c.subs[msg.ID]
	if !exists {
		return
	}
	a.removeSubscription(c, sub)
	delete(c.subs, msg.ID)
	metrics.GraphQLSubscriptionsActive.Dec()
}

// handlePing responds to a client ping with a pong.
func (a *Adapter) handlePing(c *client) {
	pong := Message{Type: MsgPong}
	data, _ := json.Marshal(pong)
	_ = c.conn.Write(c.ctx, websocket.MessageText, data)
}

// streamSubscription reads from the subscription channel and sends next messages.
func (a *Adapter) streamSubscription(c *client, sub *subscription) {
	for {
		select {
		case <-c.ctx.Done():
			return
		case ev, ok := <-sub.ch:
			if !ok {
				// Channel closed — send complete.
				complete := Message{ID: sub.id, Type: MsgComplete}
				data, _ := json.Marshal(complete)
				_ = c.conn.Write(c.ctx, websocket.MessageText, data)
				return
			}

			eventData := a.projectEvent(ev, sub.selections)
			payload, err := json.Marshal(nextPayload{Data: map[string]any{"events": eventData}})
			if err != nil {
				a.logger.Warn("marshal next payload failed", "error", err)
				continue
			}

			msg := Message{
				ID:      sub.id,
				Type:    MsgNext,
				Payload: payload,
			}
			data, _ := json.Marshal(msg)
			if err := c.conn.Write(c.ctx, websocket.MessageText, data); err != nil {
				return
			}
			metrics.GraphQLEventsSent.Inc()
		}
	}
}

// projectEvent builds the event data map, applying field projections.
func (a *Adapter) projectEvent(ev event.Event, selections []string) map[string]any {
	ev.EnsurePayload()

	result := map[string]any{
		"id":         ev.ID,
		"channel":    ev.Channel,
		"operation":  ev.Operation,
		"source":     ev.Source,
		"created_at": ev.CreatedAt.Format(time.RFC3339Nano),
	}

	if ev.Payload != nil {
		result["payload"] = json.RawMessage(ev.Payload)
	}

	// If selections are specified, filter to only requested fields.
	if len(selections) > 0 {
		filtered := make(map[string]any, len(selections))
		for _, s := range selections {
			if v, ok := result[s]; ok {
				filtered[s] = v
			}
		}
		return filtered
	}

	return result
}

// sendError sends an error message for a subscription.
func (a *Adapter) sendError(c *client, id string, message string) {
	errPayload, _ := json.Marshal([]errorPayload{{Message: message}})
	msg := Message{
		ID:      id,
		Type:    MsgError,
		Payload: errPayload,
	}
	data, _ := json.Marshal(msg)
	_ = c.conn.Write(c.ctx, websocket.MessageText, data)
}

// runKeepalive sends periodic ping messages to keep the connection alive.
func (a *Adapter) runKeepalive(c *client) {
	if a.keepaliveInterval <= 0 {
		<-c.ctx.Done()
		return
	}

	ticker := time.NewTicker(a.keepaliveInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			ping := Message{Type: MsgPing}
			data, _ := json.Marshal(ping)
			if err := c.conn.Write(c.ctx, websocket.MessageText, data); err != nil {
				return
			}
		}
	}
}
