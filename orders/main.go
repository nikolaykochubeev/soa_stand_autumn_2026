// Orders persists work before contacting other services.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const schema = `
CREATE TABLE IF NOT EXISTS orders (
    id           TEXT PRIMARY KEY,
    user_id      TEXT NOT NULL,
    status       TEXT NOT NULL,
    amount_cents BIGINT NOT NULL,
    items        JSONB NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    next_attempt TIMESTAMPTZ NOT NULL DEFAULT now(),
    event_sent   BOOLEAN NOT NULL DEFAULT false
);
CREATE INDEX IF NOT EXISTS orders_pending ON orders (next_attempt) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS orders_outbox ON orders (created_at) WHERE status = 'paid' AND NOT event_sent;
CREATE TABLE IF NOT EXISTS deliveries (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    points BIGINT NOT NULL CHECK (points > 0)
);
CREATE INDEX IF NOT EXISTS deliveries_user ON deliveries (user_id);
`

type Item struct {
	SKU        string `json:"sku"`
	Qty        int    `json:"qty"`
	PriceCents int64  `json:"price_cents"`
}

type OrderRequest struct {
	UserID string `json:"user_id"`
	Items  []Item `json:"items"`
}

type app struct {
	db              *pgxpool.Pool
	paymentURL      string
	inventoryURL    string
	eventsURL       string
	paymentMu       sync.Mutex
	paymentRetry    time.Time
	paymentProbe    bool
	paymentFailures int
}

func main() {
	a := &app{
		paymentURL:   strings.TrimRight(mustEnv("PAYMENT_URL"), "/"),
		inventoryURL: strings.TrimRight(mustEnv("INVENTORY_URL"), "/"),
		eventsURL:    strings.TrimRight(os.Getenv("EVENTS_URL"), "/"),
	}
	db, err := connect(mustEnv("DATABASE_URL"))
	if err != nil {
		slog.Error("база", "err", err)
		os.Exit(1)
	}
	a.db = db
	defer db.Close()
	go a.workOrders(context.Background())
	go a.publishEvents(context.Background())

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("POST /orders", a.createOrder)
	mux.HandleFunc("GET /orders/{id}", a.getOrder)
	mux.HandleFunc("POST /webhooks/delivery", a.delivery)
	mux.HandleFunc("GET /users/{id}/bonus", a.bonus)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	slog.Info("orders: слушаю", "port", port)
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		slog.Error("server", "err", err)
		os.Exit(1)
	}
}

// connect ждёт базу: она может подниматься дольше сервиса.
func connect(url string) (*pgxpool.Pool, error) {
	var lastErr error
	for range 60 {
		db, err := pgxpool.New(context.Background(), url)
		if err == nil {
			if _, err = db.Exec(context.Background(), schema); err == nil {
				return db, nil
			}
			db.Close()
		}
		lastErr = err
		slog.Info("база недоступна, жду", "err", err)
		time.Sleep(time.Second)
	}
	return nil, lastErr
}

func (a *app) createOrder(w http.ResponseWriter, r *http.Request) {
	var req OrderRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	if err := decoder.Decode(&req); err != nil || !valid(req) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad_request"})
		return
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad_request"})
		return
	}
	id := newID()
	var amount int64
	for _, it := range req.Items {
		amount += int64(it.Qty) * it.PriceCents
	}
	items, _ := json.Marshal(req.Items)
	ctx, cancel := context.WithTimeout(r.Context(), 800*time.Millisecond)
	defer cancel()
	_, err := a.db.Exec(ctx,
		"INSERT INTO orders (id, user_id, status, amount_cents, items) VALUES ($1, $2, 'pending', $3, $4)",
		id, req.UserID, amount, items)
	if err != nil {
		a.fail(w, id, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"id": id, "status": "pending"})
}

func (a *app) fail(w http.ResponseWriter, id string, err error) {
	slog.Error("POST /orders", "id", id, "err", err)
	writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal", "message": err.Error()})
}

func (a *app) getOrder(w http.ResponseWriter, r *http.Request) {
	var (
		id, status string
		amount     int64
	)
	err := a.db.QueryRow(r.Context(), `SELECT id, status, amount_cents FROM orders WHERE id = $1`,
		r.PathValue("id")).Scan(&id, &status, &amount)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not_found"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"id": id, "status": status, "amount_cents": amount})
}

func (a *app) reserve(id string, items []Item) error {
	type resvItem struct {
		SKU string `json:"sku"`
		Qty int    `json:"qty"`
	}
	ri := make([]resvItem, 0, len(items))
	for _, it := range items {
		ri = append(ri, resvItem{it.SKU, it.Qty})
	}
	code, err := call(http.MethodPost, a.inventoryURL+"/reservations", map[string]any{"order_id": id, "items": ri})
	if err != nil {
		return err
	}
	if code != http.StatusOK && code != http.StatusCreated {
		return fmt.Errorf("inventory: %d", code)
	}
	return nil
}

func (a *app) release(id string) error {
	code, err := call(http.MethodDelete, a.inventoryURL+"/reservations/"+id, nil)
	if err != nil {
		return err
	}
	if code != http.StatusNoContent {
		return fmt.Errorf("release: %d", code)
	}
	return nil
}

var errPayment = errors.New("payment")

// charge списывает деньги; declined — банк отказал.
func (a *app) charge(id string, amount int64) (bool, error) {
	a.paymentMu.Lock()
	if time.Now().Before(a.paymentRetry) || a.paymentProbe {
		a.paymentMu.Unlock()
		return false, errPayment
	}
	if !a.paymentRetry.IsZero() {
		a.paymentProbe = true
	}
	a.paymentMu.Unlock()

	declined, err := a.chargeOnce(id, amount)
	if err != nil && a.paymentCaptured(id, amount) {
		err = nil
	}
	a.paymentMu.Lock()
	a.paymentProbe = false
	if err != nil {
		a.paymentFailures++
		if a.paymentFailures >= 3 {
			cooldown := 5 * time.Second
			if errors.Is(err, errPayment) {
				// An answering bank needs shorter backoff than a hanging one.
				cooldown = 500 * time.Millisecond
			}
			a.paymentRetry = time.Now().Add(cooldown)
		}
	} else {
		a.paymentFailures = 0
		a.paymentRetry = time.Time{}
	}
	a.paymentMu.Unlock()
	return declined, err
}

func (a *app) chargeOnce(id string, amount int64) (bool, error) {
	code, err := call(http.MethodPost, a.paymentURL+"/payments", map[string]any{
		"order_id":     id,
		"amount_cents": amount,
		"currency":     "RUB",
	}, id)
	if err != nil {
		return false, err
	}
	switch code {
	case http.StatusOK, http.StatusCreated:
		return false, nil
	case http.StatusPaymentRequired:
		return true, nil
	}
	return false, fmt.Errorf("%w: %d", errPayment, code)
}

func call(method, url string, body any, key ...string) (int, error) {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, url, rd)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	if len(key) > 0 {
		req.Header.Set("Idempotency-Key", key[0])
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, nil
}

func valid(r OrderRequest) bool {
	if r.UserID == "" || len(r.Items) == 0 {
		return false
	}
	var total int64
	for _, it := range r.Items {
		if it.SKU == "" || it.Qty < 1 || it.PriceCents < 0 {
			return false
		}
		if it.PriceCents > (math.MaxInt64-total)/int64(it.Qty) {
			return false
		}
		total += int64(it.Qty) * it.PriceCents
	}
	return true
}

func newID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func mustEnv(k string) string {
	v := os.Getenv(k)
	if v == "" {
		slog.Error("не задана переменная окружения", "name", k)
		os.Exit(1)
	}
	return v
}
