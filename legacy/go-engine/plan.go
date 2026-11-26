package engine

import (
	"errors"
	"fmt"
	"math/big"
)

type Signal struct {
	Quantity string
	Price    string
}

type Market struct {
	Bid, Ask, Mark                 string
	Step, MinQuantity, MaxQuantity string
	MinNotional                    string
}

type PlanInput struct {
	Key                                                           OrderKey
	Action                                                        string
	Signal                                                        Signal
	Cumulative                                                    *Signal
	AlreadyFilled, LeaderBefore, Attributed                       string
	ActualPosition, OppositePosition                              string
	LeaderBalance, WalletBalance, Available, Leverage, Multiplier string
	HedgeMode                                                     bool
	Market                                                        Market
}

type MarketOrder struct {
	Symbol, Side, PositionSide, Quantity string
	ReduceOnly                           bool
}

func PlanOrder(in PlanInput) (MarketOrder, error) {
	side, direction := in.Key.Side, in.Key.PositionSide
	if (side != "BUY" && side != "SELL") || (direction != "LONG" && direction != "SHORT") || in.Key.Symbol == "" {
		return MarketOrder{}, errors.New("订单方向无效")
	}

	bid, err := positiveDecimal(in.Market.Bid, "bid")
	if err != nil {
		return MarketOrder{}, err
	}
	ask, err := positiveDecimal(in.Market.Ask, "ask")
	if err != nil {
		return MarketOrder{}, err
	}
	reference, err := positiveDecimal(in.Signal.Price, "leader trade price")
	if err != nil {
		return MarketOrder{}, err
	}
	if bid.Cmp(ask) > 0 {
		return MarketOrder{}, errors.New("买一价高于卖一价")
	}
	price := bid
	if side == "BUY" {
		price = ask
	}
	deviation := new(big.Rat).Sub(price, reference)
	deviation.Abs(deviation).Quo(deviation, reference)
	if deviation.Cmp(big.NewRat(3, 100)) > 0 {
		return MarketOrder{}, errors.New("价格偏离超过 3%")
	}

	step, err := positiveDecimal(in.Market.Step, "quantity step")
	if err != nil {
		return MarketOrder{}, err
	}
	minimum, err := decimal(in.Market.MinQuantity)
	if err != nil {
		return MarketOrder{}, fmt.Errorf("市价单数量规则无效")
	}
	maximum, err := positiveDecimal(in.Market.MaxQuantity, "maximum quantity")
	if err != nil {
		return MarketOrder{}, err
	}
	if !new(big.Rat).Quo(minimum, step).IsInt() {
		return MarketOrder{}, errors.New("交易对最小数量与步长不兼容")
	}
	minimum = larger(minimum, step)

	order := MarketOrder{Symbol: in.Key.Symbol, Side: side, PositionSide: "BOTH"}
	if in.HedgeMode {
		order.PositionSide = direction
	}
	var quantity *big.Rat
	switch in.Action {
	case "open":
		quantity, err = planOpen(in, price, step, minimum)
	case "close":
		quantity, err = planClose(in, step, minimum)
		order.ReduceOnly = !in.HedgeMode
	case "unmatched_close":
		return MarketOrder{}, errors.New("无法确定带单员平仓前持仓，无法计算平仓比例，请核对")
	default:
		return MarketOrder{}, errors.New("无效跟单操作")
	}
	if err != nil {
		return MarketOrder{}, err
	}
	if quantity.Cmp(minimum) < 0 {
		return MarketOrder{}, errors.New("跟单数量低于最小数量")
	}
	if quantity.Cmp(maximum) > 0 {
		return MarketOrder{}, errors.New("跟单数量超过最大数量")
	}
	scale, _ := step.FloatPrec()
	order.Quantity = quantity.FloatString(scale)
	return order, nil
}

func planOpen(in PlanInput, price, step, minimum *big.Rat) (*big.Rat, error) {
	if !in.HedgeMode {
		opposite, err := decimal(in.OppositePosition)
		if err != nil {
			return nil, fmt.Errorf("持仓数据无效")
		}
		if opposite.Sign() > 0 {
			return nil, errors.New("单向持仓存在反向仓位，无法按方向开仓")
		}
	}
	source := in.Signal
	if in.Cumulative != nil {
		source = *in.Cumulative
	}
	multiplier := in.Multiplier
	if multiplier == "" {
		multiplier = "1"
	}
	names := []string{"leader quantity", "leader price", "wallet balance", "leader balance", "multiplier", "mark price", "leverage"}
	values := []string{source.Quantity, source.Price, in.WalletBalance, in.LeaderBalance, multiplier, in.Market.Mark, in.Leverage}
	parsed := make([]*big.Rat, len(values))
	for i, value := range values {
		var err error
		parsed[i], err = positiveDecimal(value, names[i])
		if err != nil {
			return nil, err
		}
	}
	quantity := new(big.Rat).Mul(parsed[0], parsed[1])
	quantity.Mul(quantity, parsed[2]).Mul(quantity, parsed[4])
	quantity.Quo(quantity, parsed[3]).Quo(quantity, price)

	mark, leverage := parsed[5], parsed[6]
	notional, err := decimal(in.Market.MinNotional)
	if err != nil {
		return nil, errors.New("最小名义金额规则无效")
	}
	minimum = larger(minimum, roundStep(new(big.Rat).Quo(notional, mark), step, true))
	quantity = new(big.Rat).Set(larger(roundStep(quantity, step, false), minimum))
	if in.Cumulative != nil {
		filled, err := decimal(in.AlreadyFilled)
		if err != nil {
			return nil, errors.New("已成交数量无效，无法计算增量")
		}
		quantity.Sub(quantity, filled)
		if quantity.Sign() <= 0 {
			return nil, errors.New("累计跟单数量已达到目标，跳过增量")
		}
		quantity = roundStep(quantity, step, false)
		if quantity.Cmp(minimum) < 0 {
			return nil, errors.New("增量不足最小下单量，暂不加仓")
		}
	}
	available, err := decimal(in.Available)
	if err != nil {
		return nil, fmt.Errorf("未获取到有效的 USDT 钱包余额或可用余额")
	}
	margin := new(big.Rat).Mul(quantity, larger(price, mark))
	if margin.Quo(margin, leverage).Cmp(available) > 0 {
		return nil, errors.New("跟单所需保证金超过可用余额")
	}
	return quantity, nil
}

func planClose(in PlanInput, step, minimum *big.Rat) (*big.Rat, error) {
	actual, err := positiveDecimal(in.ActualPosition, "actual position")
	if err != nil {
		return nil, err
	}
	attributed, err := positiveDecimal(in.Attributed, "attributed position")
	if err != nil {
		return nil, err
	}
	before, err := positiveDecimal(in.LeaderBefore, "leader position before reduction")
	if err != nil {
		return nil, err
	}
	reduction, err := positiveDecimal(in.Signal.Quantity, "leader reduction")
	if err != nil {
		return nil, err
	}
	if reduction.Cmp(before) > 0 {
		return nil, errors.New("带单员减仓数量超过平仓前持仓")
	}
	limit := actual
	if attributed.Cmp(actual) < 0 {
		limit = attributed
	}
	limit = roundStep(limit, step, false)
	if limit.Cmp(minimum) < 0 {
		return nil, errors.New("可平仓数量低于交易对最小数量")
	}
	quantity := new(big.Rat).Mul(attributed, reduction)
	quantity = larger(roundStep(quantity.Quo(quantity, before), step, true), minimum)
	if quantity.Cmp(limit) > 0 {
		return limit, nil
	}
	return quantity, nil
}

func positiveDecimal(value, name string) (*big.Rat, error) {
	n, err := decimal(value)
	if err != nil || n.Sign() <= 0 {
		return nil, fmt.Errorf("%s must be a positive decimal", name)
	}
	return n, nil
}

func larger(a, b *big.Rat) *big.Rat {
	if a.Cmp(b) < 0 {
		return b
	}
	return a
}

func roundStep(quantity, step *big.Rat, up bool) *big.Rat {
	ratio := new(big.Rat).Quo(quantity, step)
	units, remainder := new(big.Int), new(big.Int)
	units.QuoRem(ratio.Num(), ratio.Denom(), remainder)
	if up && remainder.Sign() > 0 {
		units.Add(units, big.NewInt(1))
	}
	return new(big.Rat).Mul(new(big.Rat).SetInt(units), step)
}
