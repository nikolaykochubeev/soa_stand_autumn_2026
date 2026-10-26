package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func testDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set TEST_DATABASE_URL to a disposable Postgres database")
	}
	db, err := connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	if _, err = db.Exec(context.Background(), `TRUNCATE orders, deliveries`); err != nil {
		t.Fatal(err)
	}
	return db
}

func TestOrderAcceptedBeforeSlowPayment(t *testing.T) {
	db := testDB(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/payments" {
			time.Sleep(1200 * time.Millisecond)
		}
		w.WriteHeader(201)
	}))
	defer srv.Close()
	a := &app{db: db, paymentURL: srv.URL, inventoryURL: srv.URL}
	w := httptest.NewRecorder()
	start := time.Now()
	a.createOrder(w, httptest.NewRequest("POST", "/orders", strings.NewReader(`{"user_id":"u-1","items":[{"sku":"sku-1","qty":2,"price_cents":100}]}`)))
	if elapsed := time.Since(start); elapsed >= time.Second {
		t.Fatalf("response took %v", elapsed)
	}
	if w.Code != 201 || !strings.Contains(w.Body.String(), `"pending"`) {
		t.Fatalf("response: %d %s", w.Code, w.Body.String())
	}
	var status string
	var amount int64
	if err := db.QueryRow(context.Background(), `SELECT status,amount_cents FROM orders`).Scan(&status, &amount); err != nil {
		t.Fatal(err)
	}
	if status != "pending" || amount != 200 {
		t.Fatalf("persisted status=%s amount=%d", status, amount)
	}
}

func TestPendingOrderAndOutboxSurviveRestart(t *testing.T) {
	db := testDB(t)
	unavailable := true
	var ids []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/events" {
			var event struct {
				ID      string `json:"id"`
				Type    string `json:"type"`
				OrderID string `json:"order_id"`
			}
			_ = json.NewDecoder(r.Body).Decode(&event)
			if event.Type != "order.paid" || event.OrderID == "" {
				t.Error("invalid event")
			}
			ids = append(ids, event.ID)
			if unavailable {
				w.WriteHeader(503)
			} else {
				w.WriteHeader(202)
			}
			return
		}
		w.WriteHeader(201)
	}))
	defer srv.Close()
	first := &app{db: db, paymentURL: srv.URL, inventoryURL: srv.URL, eventsURL: srv.URL}
	w := httptest.NewRecorder()
	first.createOrder(w, httptest.NewRequest("POST", "/orders", strings.NewReader(`{"user_id":"u-1","items":[{"sku":"a","qty":1,"price_cents":100}]}`)))
	restarted := &app{db: db, paymentURL: srv.URL, inventoryURL: srv.URL, eventsURL: srv.URL}
	restarted.processBatch(context.Background())
	var status string
	var sent bool
	if err := db.QueryRow(context.Background(), `SELECT status,event_sent FROM orders`).Scan(&status, &sent); err != nil {
		t.Fatal(err)
	}
	if status != "paid" || sent {
		t.Fatalf("status=%s sent=%v", status, sent)
	}
	restarted.publishBatch(context.Background())
	unavailable = false
	restarted = &app{db: db, eventsURL: srv.URL}
	restarted.publishBatch(context.Background())
	if err := db.QueryRow(context.Background(), `SELECT event_sent FROM orders`).Scan(&sent); err != nil {
		t.Fatal(err)
	}
	if !sent || len(ids) != 2 || ids[0] == "" || ids[0] != ids[1] {
		t.Fatalf("sent=%v event IDs=%v", sent, ids)
	}
}

func TestConcurrentDeliveryCopiesCountOnce(t *testing.T) {
	db := testDB(t)
	a := &app{db: db}
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			w := httptest.NewRecorder()
			a.delivery(w, httptest.NewRequest("POST", "/webhooks/delivery", strings.NewReader(`{"id":"delivery-1","type":"delivery.confirmed","user_id":"u-3","bonus_points":40}`)))
			if w.Code != 204 {
				t.Errorf("webhook response=%d %s", w.Code, w.Body.String())
			}
		})
	}
	wg.Wait()
	restarted := &app{db: db}
	for _, tc := range []struct{ user, want string }{{"u-3", `"points":40`}, {"u-0", `"points":0`}} {
		req := httptest.NewRequest("GET", "/users/"+tc.user+"/bonus", nil)
		req.SetPathValue("id", tc.user)
		w := httptest.NewRecorder()
		restarted.bonus(w, req)
		if w.Code != 200 || !strings.Contains(w.Body.String(), tc.want) {
			t.Fatalf("bonus %s: %d %s", tc.user, w.Code, w.Body.String())
		}
	}
}
