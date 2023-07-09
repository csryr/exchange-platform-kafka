package backendexchange

import (
	"sort"
	"sync"
)

func (book *OrderBook) Process(order Order, mu *sync.Mutex) []Trade {
	mu.Lock()
	defer mu.Unlock()
	if order.Side == 1 {
		return book.processLimitBuy(order)
	}
	return book.processLimitSell(order)
}

func (book *OrderBook) processLimitBuy(order Order) []Trade {
	trades := make([]Trade, 0, 1)
	n := len(book.SellOrders)
	if n != 0 && book.SellOrders[n-1].Price <= order.Price {
		i := sort.Search(n, func(i int) bool { return book.SellOrders[i].Price <= order.Price })
		if i < n {
			for ; i < n; i++ {
				sellOrder := book.SellOrders[i]
				if sellOrder.Amount >= order.Amount {
					trades = append(trades, Trade{order.ID, sellOrder.ID, order.Amount, sellOrder.Price, order.ExchangeType})
					sellOrder.Amount -= order.Amount
					if sellOrder.Amount == 0 {
						book.removeSellOrder(i)
						i-- // Decrement i to account for the removed element
						n-- // Decrement n to account for the removed element
					}
					return trades
				}
				if sellOrder.Amount < order.Amount {
					trades = append(trades, Trade{order.ID, sellOrder.ID, sellOrder.Amount, sellOrder.Price, order.ExchangeType})
					order.Amount -= sellOrder.Amount
					book.removeSellOrder(i)
					i-- // Decrement i to account for the removed element
					n-- // Decrement n to account for the removed element
					continue
				}
			}
		}
	}
	book.addBuyOrder(order)
	return trades
}

func (book *OrderBook) processLimitSell(order Order) []Trade {
	trades := make([]Trade, 0, 1)
	n := len(book.BuyOrders)
	if n != 0 && book.BuyOrders[n-1].Price >= order.Price {
		i := sort.Search(n, func(i int) bool { return book.BuyOrders[i].Price >= order.Price })
		if i < n {
			for ; i < n; i++ {
				buyOrder := book.BuyOrders[i]
				if buyOrder.Amount >= order.Amount {
					trades = append(trades, Trade{order.ID, buyOrder.ID, order.Amount, buyOrder.Price, order.ExchangeType})
					buyOrder.Amount -= order.Amount
					if buyOrder.Amount == 0 {
						book.removeBuyOrder(i)
						i-- // Decrement i to account for the removed element
						n-- // Decrement n to account for the removed element
					}
					return trades
				}
				if buyOrder.Amount < order.Amount {
					trades = append(trades, Trade{order.ID, buyOrder.ID, buyOrder.Amount, buyOrder.Price, order.ExchangeType})
					order.Amount -= buyOrder.Amount
					book.removeBuyOrder(i)
					i-- // Decrement i to account for the removed element
					n-- // Decrement n to account for the removed element
					continue
				}
			}
		}
	}
	book.addSellOrder(order)
	return trades
}
