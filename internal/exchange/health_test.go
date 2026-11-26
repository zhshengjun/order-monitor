package exchange

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCheckAccountReadsTradeAndWithdrawPermissions(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/fapi/v2/account" {
			http.NotFound(w, r)
			return
		}
		fmt.Fprint(w, `{"canTrade":true,"canWithdraw":false,"canDeposit":true,"totalWalletBalance":"100"}`)
	}))
	defer server.Close()
	status, err := CheckAccountAt(context.Background(), server.Client(), server.URL, "key", "secret")
	if err != nil {
		t.Fatal(err)
	}
	if !status.CanTrade || status.CanWithdraw || !status.CanDeposit || status.TotalWalletBalance != "100" {
		t.Fatalf("status %+v", status)
	}
}
