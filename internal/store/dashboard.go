package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
)

var dashboardQueries = map[string]string{
	"targets":                  `SELECT t.* FROM targets t LEFT JOIN display_order d ON d.kind='targets' AND d.item_id=t.id ORDER BY d.position IS NULL,d.position,t.name,t.id`,
	"futures_accounts":         `SELECT a.id,a.name FROM futures_accounts a LEFT JOIN display_order d ON d.kind='accounts' AND d.item_id=CAST(a.id AS TEXT) ORDER BY d.position IS NULL,d.position,a.id`,
	"copy_bindings":            `SELECT b.account_id,b.target_id,COALESCE(m.multiplier,'1') AS multiplier FROM copy_bindings b LEFT JOIN copy_multiplier_settings_v2 m ON m.account_id=b.account_id AND m.target_id=b.target_id`,
	"virtual_sessions":         `SELECT account_id,target_id,active,started_at FROM virtual_sessions`,
	"virtual_positions":        `SELECT account_id,target_id,symbol,position_side,quantity FROM virtual_positions WHERE quantity != '0'`,
	"copied_positions":         `SELECT p.account_id,p.target_id,p.symbol,p.position_side,p.quantity,COALESCE(t.name,p.target_id) AS target_name FROM copied_positions p LEFT JOIN targets t ON t.id=p.target_id WHERE p.quantity != '0'`,
	"copy_live":                `SELECT account_id,target_id,enabled FROM copy_live`,
	"copy_tasks":               `SELECT c.id,c.account_id,c.target_id,c.state,c.error,c.created,c.result,c.body,a.balance_time,a.asset,a.wallet_balance,a.available_balance,a.lead_balance,a.reference_price,a.planned_quantity FROM copy_tasks c LEFT JOIN copy_task_audit a ON a.task_id=c.id ORDER BY c.id DESC LIMIT 50`,
	"virtual_events":           `SELECT account_id,target_id,symbol,position_side,stamp,action,quantity FROM virtual_events WHERE action='unmatched_close' ORDER BY stamp DESC LIMIT 20`,
	"virtual_trust":            `SELECT account_id,target_id,trusted,reason,updated FROM virtual_trust`,
	"active_alerts":            `SELECT key,scope,severity,title,message,count,first_seen,last_seen FROM alerts WHERE status='active' ORDER BY CASE severity WHEN 'critical' THEN 0 WHEN 'warning' THEN 1 ELSE 2 END,last_seen DESC`,
	"account_health":           `SELECT a.id AS account_id,a.name,h.ok AS ok,h.can_trade AS can_trade,h.can_withdraw AS can_withdraw,h.checked_at AS checked_at,COALESCE(h.error,'') AS error FROM futures_accounts a LEFT JOIN account_health h ON h.account_id=a.id`,
	"position_reconciliation":  `SELECT account_id,symbol,position_side,actual_quantity,local_quantity,difference,status,checked_at,acknowledged FROM position_reconciliation ORDER BY CASE status WHEN 'over' THEN 0 WHEN 'unattributed' THEN 1 ELSE 2 END,account_id,symbol`,
	"risk_breakers":            `SELECT scope,reason,active,updated FROM risk_breakers WHERE active=1`,
	"notification_errors":      `SELECT channel,attempts,error FROM outbox WHERE status='pending' AND error!='' ORDER BY id DESC LIMIT 5`,
	"unresolved_orders":        `SELECT c.id,c.body,c.error,a.name AS account_name,t.name AS target_name FROM copy_tasks c JOIN futures_accounts a ON a.id=c.account_id JOIN targets t ON t.id=c.target_id WHERE c.state='submitted' ORDER BY c.id`,
	"unresolved_manual_closes": `SELECT m.id,m.account_id,m.symbol,m.side,m.state,m.error,a.name AS account_name FROM manual_closes m JOIN futures_accounts a ON a.id=m.account_id WHERE m.state IN ('submitted','needs_review') ORDER BY m.created`,
}

func dashboardRows(ctx context.Context, tx *sql.Tx, query string, args ...any) ([]map[string]any, error) {
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	result := make([]map[string]any, 0)
	for rows.Next() {
		values := make([]any, len(columns))
		dest := make([]any, len(columns))
		for i := range values {
			dest[i] = &values[i]
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}
		item := make(map[string]any, len(columns))
		for i, col := range columns {
			item[col] = values[i]
		}
		result = append(result, item)
	}
	return result, rows.Err()
}
func (s *SQLiteStore) Dashboard(ctx context.Context) (map[string]any, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	result := make(map[string]any)
	for name, query := range dashboardQueries {
		rows, err := dashboardRows(ctx, tx, query)
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			switch name {
			case "targets":
				var detail map[string]any
				raw, _ := row["detail"].(string)
				if err = json.Unmarshal([]byte(raw), &detail); err != nil {
					return nil, err
				}
				row["detail"] = detail
			case "unresolved_orders":
				raw, _ := row["body"].(string)
				var body struct{ Symbol string }
				if err = json.Unmarshal([]byte(raw), &body); err != nil {
					return nil, err
				}
				delete(row, "body")
				row["symbol"] = body.Symbol
			}
		}
		result[name] = rows
	}
	counts, err := dashboardRows(ctx, tx, "SELECT status,COUNT(*) AS n FROM outbox GROUP BY status")
	if err != nil {
		return nil, err
	}
	notifications := map[string]any{}
	for _, r := range counts {
		notifications[r["status"].(string)] = r["n"]
	}
	result["notifications"] = notifications
	settings := defaultSettings()
	var raw string
	err = tx.QueryRowContext(ctx, "SELECT value FROM settings WHERE id=1").Scan(&raw)
	if err == nil {
		if err = json.Unmarshal([]byte(raw), &settings); err != nil {
			return nil, err
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	for _, secret := range []string{"telegram_token", "bark_key"} {
		value, _ := settings[secret].(string)
		settings[secret+"_configured"] = value != ""
		delete(settings, secret)
	}
	result["settings"] = settings
	return result, tx.Commit()
}
