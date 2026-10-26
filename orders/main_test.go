package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// Without a stable key, retrying an uncertain debit creates a second charge.
func TestPaymentRetriesUseSameKey(t *testing.T) {
	var charges atomic.Int32
	var attempts atomic.Int32
	seen := make(map[string]bool)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("Idempotency-Key")
		if key == "" || !seen[key] {
			charges.Add(1)
			seen[key] = true
		}
		if attempts.Add(1) == 1 {
			w.WriteHeader(500)
			return
		}
		w.WriteHeader(200)
	}))
	defer srv.Close()
	a := &app{paymentURL: srv.URL}
	_, _ = a.chargeOnce("order-1", 100)
	declined, err := a.chargeOnce("order-1", 100)
	if err != nil || declined || charges.Load() != 1 {
		t.Fatalf("declined=%v err=%v charges=%d", declined, err, charges.Load())
	}
}

func TestPaymentReconcilesLostResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			_ = json.NewEncoder(w).Encode([]map[string]any{{"charge_id": "c-1", "order_id": "order-1", "amount_cents": 100, "currency": "RUB", "status": "captured"}})
			return
		}
		c, _, _ := w.(http.Hijacker).Hijack()
		_ = c.Close()
	}))
	defer srv.Close()
	a := &app{paymentURL: srv.URL}
	declined, err := a.charge("order-1", 100)
	if err != nil || declined {
		t.Fatalf("captured payment must succeed: declined=%v err=%v", declined, err)
	}
}

func TestPaymentOutageDoesNotAmplifyRequests(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			fmt.Fprint(w, `[]`)
			return
		}
		calls.Add(1)
		w.WriteHeader(500)
	}))
	defer srv.Close()
	a := &app{paymentURL: srv.URL}
	for i := 0; i < 20; i++ {
		_, _ = a.charge(fmt.Sprint(i), 100)
	}
	if calls.Load() > 3 {
		t.Fatalf("outage generated %d payment requests", calls.Load())
	}
}

func TestResponsiveBankRecoversAfterTransientErrors(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			fmt.Fprint(w, `[]`)
			return
		}
		if calls.Add(1) <= 3 {
			w.WriteHeader(500)
			return
		}
		w.WriteHeader(201)
	}))
	defer srv.Close()
	a := &app{paymentURL: srv.URL}
	for range 3 {
		_, _ = a.charge("order-1", 100)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if declined, err := a.charge("order-1", 100); err == nil && !declined {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("responsive bank stayed blocked after transient errors")
}
