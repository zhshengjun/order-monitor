package exchange

import (
	"context"
	"net/http"
)

type AccountStatus struct {
	CanTrade           bool   `json:"canTrade"`
	CanWithdraw        bool   `json:"canWithdraw"`
	CanDeposit         bool   `json:"canDeposit"`
	TotalWalletBalance string `json:"totalWalletBalance"`
	MultiAssetsMargin  bool   `json:"multiAssetsMargin"`
}

// CheckAccount validates an API key and returns the futures account
// permissions without changing anything.
func CheckAccount(ctx context.Context, client *http.Client, key, secret string) (AccountStatus, error) {
	return CheckAccountAt(ctx, client, "https://fapi.binance.com", key, secret)
}

// CheckAccountAt is the testable form with an explicit HTTPS origin.
func CheckAccountAt(ctx context.Context, client *http.Client, base, key, secret string) (AccountStatus, error) {
	var status AccountStatus
	c, err := NewFuturesClient(client, base, key, secret)
	if err != nil {
		return status, err
	}
	if err = c.Request(ctx, http.MethodGet, "/fapi/v2/account", nil, true, &status); err != nil {
		return status, err
	}
	return status, nil
}
