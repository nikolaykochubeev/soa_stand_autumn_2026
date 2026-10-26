package main

import (
	"encoding/json"
	"net/http"
	"net/url"
	"time"
)

var client = &http.Client{Timeout: 3 * time.Second}

// A timeout or EOF does not tell us whether the bank captured the payment.
func (a *app) paymentCaptured(id string, amount int64) bool {
	resp, err := client.Get(a.paymentURL + "/payments?order_id=" + url.QueryEscape(id))
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false
	}
	var charges []struct {
		OrderID string `json:"order_id"`
		Amount  int64  `json:"amount_cents"`
		Status  string `json:"status"`
	}
	if json.NewDecoder(resp.Body).Decode(&charges) != nil {
		return false
	}
	for _, charge := range charges {
		if charge.OrderID == id && charge.Amount == amount && charge.Status == "captured" {
			return true
		}
	}
	return false
}
