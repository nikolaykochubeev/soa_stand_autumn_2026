package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"
)

func (a *app) publishEvents(ctx context.Context) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		a.publishBatch(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (a *app) publishBatch(ctx context.Context) {
	if a.eventsURL == "" {
		return
	}
	rows, err := a.db.Query(ctx, `SELECT id FROM orders
		WHERE status = 'paid' AND NOT event_sent ORDER BY created_at LIMIT 32`)
	if err != nil {
		slog.Error("outbox", "err", err)
		return
	}
	var ids []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			break
		}
		ids = append(ids, id)
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		slog.Error("read outbox", "err", err)
		return
	}
	for _, id := range ids {
		code, err := call(http.MethodPost, a.eventsURL+"/events", map[string]string{
			"id": "order.paid:" + id, "type": "order.paid", "order_id": id,
		})
		if err != nil || code != http.StatusAccepted {
			return
		}
		if _, err := a.db.Exec(ctx, `UPDATE orders SET event_sent = true WHERE id = $1`, id); err != nil {
			slog.Error("mark event sent", "id", id, "err", err)
			return
		}
	}
}

func (a *app) delivery(w http.ResponseWriter, r *http.Request) {
	var event struct {
		ID     string `json:"id"`
		Type   string `json:"type"`
		UserID string `json:"user_id"`
		Points int64  `json:"bonus_points"`
	}
	if json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&event) != nil ||
		event.ID == "" || event.Type != "delivery.confirmed" || event.UserID == "" || event.Points <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad_event"})
		return
	}
	// The unique event row IS the bonus credit: no gap between dedup and credit.
	_, err := a.db.Exec(r.Context(), `INSERT INTO deliveries (id, user_id, points)
		VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING`, event.ID, event.UserID, event.Points)
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "retry_delivery"})
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *app) bonus(w http.ResponseWriter, r *http.Request) {
	var points int64
	err := a.db.QueryRow(r.Context(), `SELECT COALESCE(SUM(points), 0) FROM deliveries WHERE user_id = $1`,
		r.PathValue("id")).Scan(&points)
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "database"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]int64{"points": points})
}
