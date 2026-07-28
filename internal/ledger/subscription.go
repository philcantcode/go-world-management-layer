package ledger

import (
	"context"
	"errors"
	"io"
	"sync"
)

// Delivery is either a durable record or a subscriber-local typed gap. A local
// gap identifies durable cursors omitted from the bounded live queue; clients
// can retrieve those cursors with ReadAfter or start a new subscription.
type Delivery struct {
	Record *Record
	Gap    *Gap
}

type fanoutHub struct {
	mu            sync.Mutex
	nextID        uint64
	subscriptions map[uint64]*Subscription
	closed        bool
	closeErr      error
}

func newFanoutHub() *fanoutHub {
	return &fanoutHub{subscriptions: make(map[uint64]*Subscription)}
}

func (hub *fanoutHub) add(subscription *Subscription) (uint64, error) {
	hub.mu.Lock()
	defer hub.mu.Unlock()
	if hub.closed {
		if hub.closeErr != nil {
			return 0, hub.closeErr
		}
		return 0, ErrClosed
	}
	hub.nextID++
	hub.subscriptions[hub.nextID] = subscription
	return hub.nextID, nil
}

func (hub *fanoutHub) remove(id uint64) {
	hub.mu.Lock()
	delete(hub.subscriptions, id)
	hub.mu.Unlock()
}

func (hub *fanoutHub) publish(record Record) {
	hub.mu.Lock()
	defer hub.mu.Unlock()
	if hub.closed {
		return
	}
	for _, subscription := range hub.subscriptions {
		subscription.enqueueRecord(record)
	}
}

func (hub *fanoutHub) close(err error) {
	hub.mu.Lock()
	if hub.closed {
		hub.mu.Unlock()
		return
	}
	hub.closed = true
	hub.closeErr = err
	subscriptions := make([]*Subscription, 0, len(hub.subscriptions))
	for _, subscription := range hub.subscriptions {
		subscriptions = append(subscriptions, subscription)
	}
	hub.subscriptions = make(map[uint64]*Subscription)
	hub.mu.Unlock()
	for _, subscription := range subscriptions {
		subscription.terminate(err)
	}
}

// Subscription is a bounded, non-blocking live queue. Producers never wait for
// a consumer. Overflow replaces omitted deliveries with an explicit merged gap.
type Subscription struct {
	mu       sync.Mutex
	queue    []Delivery
	capacity int
	notify   chan struct{}
	done     chan struct{}
	closed   bool
	err      error
	id       uint64
	hub      *fanoutHub
}

func newSubscription(capacity int, hub *fanoutHub) *Subscription {
	return &Subscription{
		capacity: capacity,
		notify:   make(chan struct{}, 1),
		done:     make(chan struct{}),
		hub:      hub,
	}
}

// Subscribe creates an ordered subscription after a durable cursor. Existing
// history is loaded under the same append-order lock used for registration, so
// the history/live handoff has no silent interval.
func (l *Ledger) Subscribe(after Cursor, capacity int) (*Subscription, error) {
	if capacity == 0 {
		capacity = l.options.SubscriberBuffer
	}
	if capacity < 1 {
		return nil, errors.New("subscription capacity must be positive")
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil, ErrClosed
	}
	if l.failed != nil {
		return nil, l.failed
	}
	if after > l.head {
		return nil, ErrCursorOutOfRange
	}
	history, err := l.readAfterLocked(after, l.head, 0)
	if err != nil {
		return nil, err
	}
	subscription := newSubscription(capacity, l.hub)
	for _, record := range history {
		subscription.enqueueRecord(record)
	}
	id, err := l.hub.add(subscription)
	if err != nil {
		return nil, err
	}
	subscription.id = id
	return subscription, nil
}

// Next waits for a durable record or explicit local gap.
func (subscription *Subscription) Next(ctx context.Context) (Delivery, error) {
	for {
		subscription.mu.Lock()
		if len(subscription.queue) > 0 {
			delivery := cloneDelivery(subscription.queue[0])
			subscription.queue = subscription.queue[1:]
			if len(subscription.queue) > 0 {
				subscription.signalLocked()
			}
			subscription.mu.Unlock()
			return delivery, nil
		}
		if subscription.closed {
			err := subscription.err
			subscription.mu.Unlock()
			if err == nil {
				err = io.EOF
			}
			return Delivery{}, err
		}
		subscription.mu.Unlock()

		select {
		case <-ctx.Done():
			return Delivery{}, ctx.Err()
		case <-subscription.notify:
		case <-subscription.done:
		}
	}
}

// Close removes the subscription without affecting the ledger.
func (subscription *Subscription) Close() error {
	if subscription.hub != nil {
		subscription.hub.remove(subscription.id)
	}
	subscription.terminate(io.EOF)
	return nil
}

func (subscription *Subscription) enqueueRecord(record Record) {
	subscription.mu.Lock()
	defer subscription.mu.Unlock()
	if subscription.closed {
		return
	}
	delivery := Delivery{Record: recordPointer(record)}
	if len(subscription.queue) < subscription.capacity {
		subscription.queue = append(subscription.queue, delivery)
		subscription.signalLocked()
		return
	}
	from, through := deliveryRange(subscription.queue[0])
	for _, queued := range subscription.queue[1:] {
		_, queuedThrough := deliveryRange(queued)
		if queuedThrough > through {
			through = queuedThrough
		}
	}
	_, incomingThrough := deliveryRange(delivery)
	if incomingThrough > through {
		through = incomingThrough
	}
	subscription.queue = []Delivery{{Gap: &Gap{
		Cause:         GapSubscriberOverflow,
		FromCursor:    from,
		ThroughCursor: through,
		Detail:        "slow subscriber queue overflow; durable replay remains available",
	}}}
	subscription.signalLocked()
}

func (subscription *Subscription) signalLocked() {
	select {
	case subscription.notify <- struct{}{}:
	default:
	}
}

func (subscription *Subscription) terminate(err error) {
	subscription.mu.Lock()
	if subscription.closed {
		subscription.mu.Unlock()
		return
	}
	subscription.closed = true
	subscription.err = err
	close(subscription.done)
	subscription.mu.Unlock()
}

func recordPointer(record Record) *Record {
	cloned := cloneRecord(record)
	return &cloned
}

func cloneDelivery(delivery Delivery) Delivery {
	if delivery.Record != nil {
		delivery.Record = recordPointer(*delivery.Record)
	}
	if delivery.Gap != nil {
		gap := *delivery.Gap
		delivery.Gap = &gap
	}
	return delivery
}

func deliveryRange(delivery Delivery) (Cursor, Cursor) {
	if delivery.Gap != nil {
		return delivery.Gap.FromCursor, delivery.Gap.ThroughCursor
	}
	if delivery.Record != nil {
		return delivery.Record.Cursor, delivery.Record.Cursor
	}
	return 0, 0
}
