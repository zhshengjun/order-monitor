package monitor

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/big"
	"net/http"
	"order-monitor/internal/exchange"
	"order-monitor/internal/store"
	"time"
)

type riskPosition struct {
	Symbol         string `json:"symbol"`
	Side           string `json:"positionSide"`
	Amount         string `json:"positionAmt"`
	Maintenance    string `json:"maintMargin"`
	IsolatedMargin string `json:"isolatedMargin"`
	Asset          string `json:"marginAsset"`
}
type marginAccount struct {
	Multi     *bool  `json:"multiAssetsMargin"`
	Wallet    string `json:"totalCrossWalletBalance"`
	Pnl       string `json:"totalCrossUnPnl"`
	Positions []struct {
		Symbol   string `json:"symbol"`
		Side     string `json:"positionSide"`
		Isolated *bool  `json:"isolated"`
	} `json:"positions"`
}
type marginRisk struct {
	maintenance, margin *big.Rat
	label               string
}

func marginRisks(account marginAccount, positions []riskPosition) (map[string]marginRisk, error) {
	modes := map[[2]string]*bool{}
	for _, p := range account.Positions {
		modes[[2]string{p.Symbol, p.Side}] = p.Isolated
	}
	risks := map[string]marginRisk{}
	cross := new(big.Rat)
	found := false
	for _, p := range positions {
		amount, err := signedDecimal(p.Amount)
		if err != nil {
			return nil, err
		}
		if amount.Sign() == 0 {
			continue
		}
		isolated := modes[[2]string{p.Symbol, p.Side}]
		if isolated == nil {
			return nil, errors.New("缺少持仓保证金模式")
		}
		maintenance, err := decimal(p.Maintenance)
		if err != nil {
			return nil, errors.New("维持保证金无效")
		}
		if *isolated {
			margin, err := signedDecimal(p.IsolatedMargin)
			if err != nil {
				return nil, err
			}
			risks["isolated:"+p.Symbol+":"+p.Side] = marginRisk{maintenance, margin, p.Symbol + " " + p.Side + " 逐仓"}
		} else {
			if p.Asset != "USDT" {
				return nil, errors.New("暂不支持非 USDT 全仓保证金风险计算")
			}
			found = true
			cross.Add(cross, maintenance)
		}
	}
	if found {
		if account.Multi == nil || *account.Multi {
			return nil, errors.New("暂不支持多资产模式全仓保证金风险计算")
		}
		wallet, err := signedDecimal(account.Wallet)
		if err != nil {
			return nil, err
		}
		pnl, err := signedDecimal(account.Pnl)
		if err != nil {
			return nil, err
		}
		risks["cross:USDT"] = marginRisk{cross, new(big.Rat).Add(wallet, pnl), "USDT 全仓"}
	}
	return risks, nil
}
func (s *Service) writeMarginEvent(ctx context.Context, tx *sql.Tx, id int64, name, message string) error {
	body, err := json.Marshal(map[string]string{"message": message, "severity": store.SeverityCritical})
	if err != nil {
		return err
	}
	res, err := tx.ExecContext(ctx, "INSERT INTO events(target,name,kind,body,stamp,historical) VALUES(?,?,'margin',?,?,0)", fmt.Sprintf("margin:%d", id), name, string(body), time.Now().UnixMilli())
	if err != nil {
		return err
	}
	event, err := res.LastInsertId()
	if err != nil {
		return err
	}
	return store.QueueEventSeverity(ctx, tx, event, store.SeverityCritical)
}
func (s *Service) checkMargin(ctx context.Context, id int64, name, key, secret string, client *http.Client) error {
	c, err := exchange.NewFuturesClient(client, "https://fapi.binance.com", key, secret)
	if err != nil {
		return err
	}
	var account marginAccount
	var positions []riskPosition
	if err = c.Request(ctx, http.MethodGet, "/fapi/v2/account", nil, true, &account); err != nil {
		return err
	}
	if err = c.Request(ctx, http.MethodGet, "/fapi/v3/positionRisk", nil, true, &positions); err != nil {
		return err
	}
	risks, err := marginRisks(account, positions)
	if err != nil {
		return err
	}
	tx, err := s.DB().BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var raw string
	err = tx.QueryRowContext(ctx, "SELECT value FROM settings WHERE id=1").Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		raw = "{}"
	} else if err != nil {
		return err
	}
	var settings map[string]any
	if err = json.Unmarshal([]byte(raw), &settings); err != nil {
		return err
	}
	if settings["telegram_enabled"] != true && settings["bark_enabled"] != true {
		_, err = tx.ExecContext(ctx, "DELETE FROM margin_alert_state WHERE account_id=?", id)
		if err != nil {
			return err
		}
		return tx.Commit()
	}
	old, err := store.DashboardRows(ctx, tx, "SELECT risk_key,active FROM margin_alert_state WHERE account_id=?", id)
	if err != nil {
		return err
	}
	previous := map[string]bool{}
	for _, r := range old {
		previous[r["risk_key"].(string)] = r["active"].(int64) != 0
	}
	for key, risk := range risks {
		high := risk.maintenance.Sign() > 0 && (risk.margin.Sign() <= 0 || new(big.Rat).Mul(risk.maintenance, big.NewRat(100, 1)).Cmp(new(big.Rat).Mul(risk.margin, big.NewRat(60, 1))) >= 0)
		if high && !previous[key] {
			ratio := "≥100%"
			if risk.margin.Sign() > 0 {
				ratio = new(big.Rat).Quo(new(big.Rat).Mul(risk.maintenance, big.NewRat(100, 1)), risk.margin).FloatString(2) + "%"
			}
			message := fmt.Sprintf("%s 保证金风险率 %s，达到 60%%\n维持保证金：%s\n保证金余额：%s", risk.label, ratio, decimalString(risk.maintenance), decimalString(risk.margin))
			if err = s.writeMarginEvent(ctx, tx, id, name, message); err != nil {
				return err
			}
		}
		if _, err = tx.ExecContext(ctx, "INSERT OR REPLACE INTO margin_alert_state VALUES(?,?,?)", id, key, high); err != nil {
			return err
		}
		delete(previous, key)
	}
	for key := range previous {
		if _, err = tx.ExecContext(ctx, "DELETE FROM margin_alert_state WHERE account_id=? AND risk_key=?", id, key); err != nil {
			return err
		}
	}
	return tx.Commit()
}
func (s *Service) RunMarginMonitor(ctx context.Context, client *http.Client) {
	failures := map[int64]string{}
	for ctx.Err() == nil {
		rows, err := s.DB().QueryContext(ctx, "SELECT id,name,api_key,api_secret FROM futures_accounts")
		type account struct {
			id                int64
			name, key, secret string
		}
		list := []account{}
		if err == nil {
			for rows.Next() {
				var a account
				if err = rows.Scan(&a.id, &a.name, &a.key, &a.secret); err != nil {
					break
				}
				list = append(list, a)
			}
			if err == nil {
				err = rows.Err()
			}
			rows.Close()
		}
		if err != nil {
			log.Printf("保证金账户查询失败：%v", err)
		}
		for _, a := range list {
			err = s.checkMargin(ctx, a.id, a.name, a.key, a.secret, client)
			if err != nil {
				select {
				case <-ctx.Done():
					return
				case <-time.After(3 * time.Second):
				}
				err = s.checkMargin(ctx, a.id, a.name, a.key, a.secret, client)
			}
			if err == nil {
				delete(failures, a.id)
				continue
			}
			if ctx.Err() != nil {
				return
			}
			reason := err.Error()
			log.Printf("账户 %d 保证金风险检查失败：%s", a.id, reason)
			if failures[a.id] != reason {
				tx, e := s.DB().BeginTx(ctx, nil)
				if e == nil {
					e = s.writeMarginEvent(ctx, tx, a.id, a.name, "保证金风险检查失败："+reason+"，请检查账户及服务日志")
					if e == nil {
						e = tx.Commit()
					} else {
						tx.Rollback()
					}
				}
				if e == nil {
					failures[a.id] = reason
				} else {
					log.Printf("保存保证金通知失败：%v", e)
				}
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(30 * time.Second):
		}
	}
}
