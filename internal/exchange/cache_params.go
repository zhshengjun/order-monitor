package exchange

import "net/url"

func urlValues(symbol string) url.Values { return url.Values{"symbol": {symbol}} }
