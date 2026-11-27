package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"math/big"
	"order-monitor/internal/domain"
	"sort"
	"strings"
	"time"
)

type LeaderHistory interface {
	History(context.Context, string, int, int) (LeaderPage, error)
}

type LeaderPoller struct {
	Source     LeaderHistory
	Store      *Service
	Comparator *Comparator
	Now        func() time.Time
}

// PollPortfolio reads pages until it reaches an unchanged overlap. All pages
// are validated before applying fills so a request failure cannot create a
// partial order group.
func (p LeaderPoller) PollPortfolio(ctx context.Context, portfolio string, pageSize int, enabledAt time.Time, initialized ...bool) error {
	firstPoll := len(initialized) == 0 || !initialized[0]
	if p.Source == nil || p.Store == nil || p.Comparator == nil || p.Now == nil || enabledAt.IsZero() || pageSize < 1 || pageSize > 100 {
		return errors.New("leader poller needs a source, store, comparator, clock and activation time")
	}
	leaderBalance := ""
	if source, ok := p.Source.(interface {
		Detail(context.Context, string) (LeaderDetail, error)
	}); ok {
		if detail, err := source.Detail(ctx, portfolio); err == nil {
			leaderBalance = detail.MarginBalance.String()
			if raw, marshalErr := json.Marshal(detail); marshalErr == nil {
				if saveErr := p.Store.SaveLeaderDetail(ctx, portfolio, detail.Name, leaderBalance, raw); saveErr != nil {
					return saveErr
				}
			}
		}
	}
	byOrder := make(map[OrderKey]map[string]*big.Rat)
	representatives := map[OrderKey]LeaderFill{}
	firstSeen := map[OrderKey]int64{}
	now := p.Now()
	cutoff := max(enabledAt.UnixMilli(), now.Add(-3*time.Minute).UnixMilli())
	stop := false
	var collected []LeaderFill
	for page := 1; page <= 50 && !stop; page++ {
		batch, err := p.Source.History(ctx, portfolio, page, pageSize)
		if err != nil {
			return err
		}
		stop = len(batch.Fills) < pageSize
		collected = append(collected, batch.Fills...)
		for _, raw := range batch.Fills {
			observation, err := raw.Observation(portfolio)
			if errors.Is(err, domain.ErrInvalidFillTime) {
				if !firstPoll {
					if alertErr := p.Store.alertInvalidFill(ctx, portfolio, raw); alertErr != nil {
						return alertErr
					}
				}
				continue
			}
			if err != nil {
				return err
			}
			key := observation.Key
			key.Symbol = strings.ToUpper(strings.TrimSpace(key.Symbol))
			key.Side = strings.ToUpper(strings.TrimSpace(key.Side))
			key.PositionSide = strings.ToUpper(strings.TrimSpace(key.PositionSide))
			if key.UpdateTime <= cutoff {
				continue
			}
			if _, exists := firstSeen[key]; !exists {
				firstSeen[key] = p.Now().UnixMilli()
			}
			current, exists := representatives[key]
			candidateKey, e := HistoryIdentity(raw)
			if e != nil {
				return e
			}
			if !exists {
				representatives[key] = raw
			} else {
				currentKey, e := HistoryIdentity(current)
				if e != nil {
					return e
				}
				if candidateKey < currentKey {
					representatives[key] = raw
				}
			}
			quantity, err := positiveDecimal(observation.Fill.Quantity, "observed quantity")
			if err != nil {
				return err
			}
			cost, err := positiveDecimal(observation.Fill.Cost, "observed cost")
			if err != nil {
				return err
			}
			group := byOrder[key]
			if group == nil {
				group = make(map[string]*big.Rat)
				byOrder[key] = group
			}
			identity := quantity.RatString()
			if previous := group[identity]; previous == nil || cost.Cmp(previous) < 0 {
				group[identity] = cost
			}
		}
		if len(batch.Fills) == 0 {
			break
		}
		last, err := batch.Fills[len(batch.Fills)-1].Observation(portfolio)
		if errors.Is(err, domain.ErrInvalidFillTime) {
			continue
		}
		if err != nil {
			return err
		}
		if last.Key.UpdateTime <= cutoff {
			stop = true
		} else if len(batch.Fills) == pageSize {
			key := last.Key
			key.Symbol = strings.ToUpper(strings.TrimSpace(key.Symbol))
			key.Side = strings.ToUpper(strings.TrimSpace(key.Side))
			key.PositionSide = strings.ToUpper(strings.TrimSpace(key.PositionSide))
			saved, err := p.Store.Load(ctx, key)
			if err != nil {
				return err
			}
			stop = saved == last.Fill
		}
	}
	if !stop {
		return errors.New("leader history exceeds catch-up page limit")
	}

	if err := p.Store.SaveLiveHistory(ctx, portfolio, collected); err != nil {
		return err
	}

	observations := make([]Observation, 0, len(byOrder))
	for key, group := range byOrder {
		if key.UpdateTime > now.Add(5*time.Second).UnixMilli() {
			return errors.New("leader fill timestamp is in the future")
		}
		quantity, cost := new(big.Rat), new(big.Rat)
		for value, amount := range group {
			increment, ok := new(big.Rat).SetString(value)
			if !ok {
				return errors.New("invalid grouped quantity")
			}
			quantity.Add(quantity, increment)
			cost.Add(cost, amount)
		}
		quantityScale, _ := quantity.FloatPrec()
		costScale, _ := cost.FloatPrec()
		observations = append(observations, Observation{Key: key, Fill: CumulativeFill{Quantity: quantity.FloatString(quantityScale), Cost: cost.FloatString(costScale)}})
	}
	sort.Slice(observations, func(i, j int) bool {
		a, b := observations[i].Key, observations[j].Key
		if a.UpdateTime != b.UpdateTime {
			return a.UpdateTime < b.UpdateTime
		}
		if a.Symbol != b.Symbol {
			return a.Symbol < b.Symbol
		}
		if a.Side != b.Side {
			return a.Side < b.Side
		}
		return a.PositionSide < b.PositionSide
	})
	for _, observation := range observations {
		representative := representatives[observation.Key]
		raw, err := json.Marshal(representative)
		if err != nil {
			return err
		}
		var record map[string]any
		decoder := json.NewDecoder(strings.NewReader(string(raw)))
		decoder.UseNumber()
		if err = decoder.Decode(&record); err != nil {
			return err
		}
		q, _ := positiveDecimal(observation.Fill.Quantity, "quantity")
		cost, _ := positiveDecimal(observation.Fill.Cost, "cost")
		record["symbol"], record["side"], record["positionSide"] = observation.Key.Symbol, observation.Key.Side, observation.Key.PositionSide
		record["executedQty"] = observation.Fill.Quantity
		record["avgPrice"] = new(big.Rat).Quo(cost, q).FloatString(40)
		record["notional"] = observation.Fill.Cost
		if err := p.Comparator.ApplyRecord(ctx, observation, leaderBalance, record, firstSeen[observation.Key]); err != nil {
			if errors.Is(err, domain.ErrFillRegression) {
				if stopErr := p.Store.StopCopyForRegression(ctx, observation); stopErr != nil {
					return stopErr
				}
				if acceptErr := p.Comparator.Accept(ctx, observation, leaderBalance, record); acceptErr != nil {
					return acceptErr
				}
				continue
			}
			return err
		}
	}
	return nil
}
