package engine

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"
)

// OrderKey identifies a group of fills from one lead portfolio. Binance's
// history feed does not expose a unique execution ID.
type OrderKey struct {
	Portfolio, Symbol, Side, PositionSide string
	UpdateTime                            int64
}

type CumulativeFill struct {
	Quantity string
	Cost     string
}

type Observation struct {
	Key  OrderKey
	Fill CumulativeFill
}

type Comparison struct {
	LeaderBalance string
	Record        map[string]any
	DiscoveredAt  int64
	Observation
	Previous      CumulativeFill
	QuantityDelta string
	CostDelta     string
}

// ComparisonStore stores the current cumulative fill and, when appropriate,
// its copy task in one transaction. Commit must reject a stale Previous value.
// Load returns the latest committed value, or the zero value for an unseen key.
type ComparisonStore interface {
	Load(context.Context, OrderKey) (CumulativeFill, error)
	Commit(context.Context, Comparison) error
}

type cachedComparison struct {
	key  OrderKey
	fill CumulativeFill
}

// Comparator has one owner. The store remains authoritative after eviction
// and across process restarts. A failed commit never updates the cache.
type Comparator struct {
	store    ComparisonStore
	capacity int
	entries  map[OrderKey]*list.Element
	recent   list.List
}

func NewComparator(store ComparisonStore, capacity int) (*Comparator, error) {
	if store == nil || capacity < 1 {
		return nil, errors.New("comparison store and positive cache capacity required")
	}
	return &Comparator{
		store: store, capacity: capacity,
		entries: make(map[OrderKey]*list.Element),
	}, nil
}

var ErrFillRegression = errors.New("cumulative fill quantity or cost regressed")

func (c *Comparator) Apply(ctx context.Context, observation Observation) error {
	return c.apply(ctx, observation, "")
}

func (c *Comparator) apply(ctx context.Context, observation Observation, leaderBalance string) error {
	return c.applyRecord(ctx, observation, leaderBalance, nil, 0)
}

func (c *Comparator) applyRecord(ctx context.Context, observation Observation, leaderBalance string, record map[string]any, discovered int64) error {
	key := &observation.Key
	key.Symbol = strings.ToUpper(strings.TrimSpace(key.Symbol))
	key.Side = strings.ToUpper(strings.TrimSpace(key.Side))
	key.PositionSide = strings.ToUpper(strings.TrimSpace(key.PositionSide))
	if key.Portfolio == "" || key.Symbol == "" || key.Side == "" || key.PositionSide == "" || key.UpdateTime <= 0 {
		return errors.New("invalid order identity")
	}
	current := observation.Fill
	element := c.entries[*key]
	if element != nil && element.Value.(cachedComparison).fill == current {
		c.recent.MoveToFront(element)
		return nil
	}

	quantity, err := decimal(current.Quantity)
	if err != nil || quantity.Sign() <= 0 {
		return errors.New("invalid cumulative quantity")
	}
	cost, err := decimal(current.Cost)
	if err != nil || cost.Sign() <= 0 {
		return errors.New("invalid cumulative cost")
	}

	var previous CumulativeFill
	if element != nil {
		previous = element.Value.(cachedComparison).fill
	} else {
		previous, err = c.store.Load(ctx, *key)
		if err != nil {
			return err
		}
	}

	change := Comparison{Observation: observation, Previous: previous, LeaderBalance: leaderBalance, Record: record, DiscoveredAt: discovered}
	if previous != (CumulativeFill{}) {
		oldQuantity, err := decimal(previous.Quantity)
		if err != nil {
			return fmt.Errorf("stored quantity: %w", err)
		}
		oldCost, err := decimal(previous.Cost)
		if err != nil {
			return fmt.Errorf("stored cost: %w", err)
		}
		quantity.Sub(quantity, oldQuantity)
		cost.Sub(cost, oldCost)
		if quantity.Sign() < 0 || quantity.Sign() > 0 && cost.Sign() <= 0 {
			return ErrFillRegression
		}
		if quantity.Sign() == 0 && cost.Sign() == 0 {
			c.remember(*key, previous, element)
			return nil
		}
	}
	if quantity.Sign() > 0 {
		quantityScale, _ := quantity.FloatPrec()
		costScale, _ := cost.FloatPrec()
		change.QuantityDelta = quantity.FloatString(quantityScale)
		change.CostDelta = cost.FloatString(costScale)
	}
	if err := c.store.Commit(ctx, change); err != nil {
		// A subsequent attempt reloads durable state even if the outcome of
		// the failed commit was unclear to the caller.
		if element != nil {
			c.recent.Remove(element)
			delete(c.entries, *key)
		}
		return err
	}
	c.remember(*key, current, element)
	return nil
}

func (c *Comparator) remember(key OrderKey, fill CumulativeFill, element *list.Element) {
	entry := cachedComparison{key, fill}
	if element != nil {
		element.Value = entry
		c.recent.MoveToFront(element)
		return
	}
	c.entries[key] = c.recent.PushFront(entry)
	if len(c.entries) > c.capacity {
		oldest := c.recent.Back()
		delete(c.entries, oldest.Value.(cachedComparison).key)
		c.recent.Remove(oldest)
	}
}

func decimal(value string) (*big.Rat, error) {
	for _, digit := range value {
		if digit != '.' && (digit < '0' || digit > '9') {
			return nil, errors.New("invalid decimal")
		}
	}
	parsed, ok := new(big.Rat).SetString(value)
	if !ok {
		return nil, errors.New("invalid decimal")
	}
	return parsed, nil
}
