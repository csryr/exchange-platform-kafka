package backendexchange

import "sort"

// OrderBook type
type OrderBook struct {
	BuyOrders  []Order
	SellOrders []Order
}

func (book *OrderBook) addBuyOrder(order Order) {
	i := sort.Search(len(book.BuyOrders), func(i int) bool { return book.BuyOrders[i].Price < order.Price })
	book.BuyOrders = append(book.BuyOrders, Order{})
	copy(book.BuyOrders[i+1:], book.BuyOrders[i:])
	book.BuyOrders[i] = order
}
func (book *OrderBook) addSellOrder(order Order) {
	i := sort.Search(len(book.SellOrders), func(i int) bool { return book.SellOrders[i].Price > order.Price })
	book.SellOrders = append(book.SellOrders, Order{})
	copy(book.SellOrders[i+1:], book.SellOrders[i:])
	book.SellOrders[i] = order
}
func (book *OrderBook) removeBuyOrder(index int) {
	book.BuyOrders = append(book.BuyOrders[:index], book.BuyOrders[index+1:]...)
}
func (book *OrderBook) removeSellOrder(index int) {
	book.SellOrders = append(book.SellOrders[:index], book.SellOrders[index+1:]...)
}
