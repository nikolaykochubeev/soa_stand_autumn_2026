package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"
)

// Only one dispatcher runs; it waits for the batch before selecting again.
// A crash leaves pending rows in Postgres for the next process.
func (a *app) workOrders(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		a.processBatch(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

type pendingOrder struct {
	id     string
	amount int64
	items  []byte
}

func (a *app) processBatch(ctx context.Context) {
	rows, err := a.db.Query(ctx, `SELECT id, amount_cents, items FROM orders
		WHERE status = 'pending' AND next_attempt <= now() ORDER BY next_attempt LIMIT 4`)
	if err != nil {
		slog.Error("pending orders", "err", err)
		return
	}
	var batch []pendingOrder
	for rows.Next() {
		var o pendingOrder
		if err = rows.Scan(&o.id, &o.amount, &o.items); err != nil {
			break
		}
		batch = append(batch, o)
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		slog.Error("read pending orders", "err", err)
		return
	}
	var wg sync.WaitGroup
	for _, o := range batch {
		wg.Go(func() {
			if err := a.processOrder(ctx, o); err != nil {
				slog.Warn("order will be retried", "id", o.id, "err", err)
				if _, err := a.db.Exec(ctx, `UPDATE orders SET next_attempt = now() + interval '1 second' WHERE id = $1`, o.id); err != nil {
					slog.Error("schedule retry", "id", o.id, "err", err)
				}
			}
		})
	}
	wg.Wait()
}

func (a *app) processOrder(ctx context.Context, o pendingOrder) error {
	var items []Item
	if err := json.Unmarshal(o.items, &items); err != nil {
		return err
	}
	if err := a.reserve(o.id, items); err != nil {
		return err
	}
	var declined bool
	if o.amount > 0 {
		var err error
		declined, err = a.charge(o.id, o.amount)
		if err != nil {
			return err
		}
	}
	status := "paid"
	if declined {
		// Do not expose rejected until the reservation is actually released.
		if err := a.release(o.id); err != nil {
			return err
		}
		status = "rejected"
	}
	// event_sent=false is the outbox marker, committed atomically with paid.
	_, err := a.db.Exec(ctx, `UPDATE orders SET status = $2 WHERE id = $1`, o.id, status)
	return err
}
