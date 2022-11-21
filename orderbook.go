package main

import (
	"bytes"
	"container/list"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/apd"
	rbtEx "github.com/emirpasic/gods/examples/redblacktreeextended"
	rbt "github.com/emirpasic/gods/trees/redblacktree"
)

// OperationType is a type of operation.
type OperationType int

const (
	Ask OperationType = iota
	Bid
)

// Asset is a name of Asset. Like BTC, USDT, etc.
type Asset string

// OrderID is a unique order id.
type OrderID string

// Order is an order in the order book.
type Order struct {
	orderID       OrderID
	operationType OperationType
	amount        *apd.Decimal
	price         *apd.Decimal

	// we store data that this order was executed or partially executed.
	executions []*ExecutionReport

	createdAt time.Time
}

// ExecutionReport is a log data of executed orders.
type ExecutionReport struct {
	initiatorOrderID OrderID
	executorOrderID  OrderID
	amount           *apd.Decimal
	price            *apd.Decimal
}

// OrderBook is a main domain for order book in matching engine.
type OrderBook struct {
	// BaseAsset is a base Asset.
	// For example for pair BTC-USDT, BTC is a BaseAsset.
	BaseAsset Asset
	// BaseAsset is a quote Asset.
	// For example for pair BTC-USDT, USDT is a QuoteAsset.
	QuoteAsset Asset

	// Storage of orders.
	Orders map[OrderID]*list.Element
	// Storage of executed orders.
	OrdersDone map[OrderID]*Order

	// Asks are sells in the order book.
	Asks *OrderSide
	// Bid are buys in the order book.
	Bids *OrderSide

	mx sync.Mutex
}

// NewOrderBook creates a new instance of OrderBook.
func NewOrderBook(baseAsset, quoteAsset Asset) *OrderBook {
	return &OrderBook{
		BaseAsset:  baseAsset,
		QuoteAsset: quoteAsset,
		Orders:     map[OrderID]*list.Element{},
		OrdersDone: map[OrderID]*Order{},
		Asks:       NewOrderSide(Ask),
		Bids:       NewOrderSide(Bid),
		mx:         sync.Mutex{},
	}
}

// OrdersBySpecificPrice we store all orders by specific price as linked list.
// If someone want's to buy or sell on this price level we take the first element of the list.
// If volume enough - finish. If not - go and take the volume form the next order, etc.
type OrdersBySpecificPrice struct {
	// is a price on this level.
	price *apd.Decimal
	// totalAmount is a total amount of volume on this price level
	totalAmount *apd.Decimal
	// linked listed of the orders on this price level.
	orders *list.List
}

// NewOrdersBySpecificPrice creates a new instance of OrdersBySpecificPrice.
func NewOrdersBySpecificPrice(price, amount *apd.Decimal) *OrdersBySpecificPrice {
	return &OrdersBySpecificPrice{
		price:       price,
		totalAmount: amount,
		orders:      list.New(),
	}
}

// AddAmount adds amount.
func (op *OrdersBySpecificPrice) AddAmount(amount *apd.Decimal) error {
	d := apd.New(0, 0)

	_, err := apd.BaseContext.Add(d, op.totalAmount, amount)
	op.totalAmount = d

	return err
}

// OrderSide is a part of order book, there are 2 sides: asks (sells in the order book) and bids (buys in the order book).
type OrderSide struct {
	priceTree *rbtEx.RedBlackTreeExtended
	// cache of the prices, where string is a price
	prices map[string]*OrdersBySpecificPrice

	sideType OperationType
}

// NewOrderSide creates a new instance of the OrderSide.
func NewOrderSide(sideType OperationType) *OrderSide {
	f := func(a, b interface{}) int {
		return a.(*apd.Decimal).Cmp(b.(*apd.Decimal))
	}

	return &OrderSide{
		priceTree: &rbtEx.RedBlackTreeExtended{
			Tree: rbt.NewWith(f),
		},
		prices:   map[string]*OrdersBySpecificPrice{},
		sideType: sideType,
	}
}

// String returns the string.
func (os *OrderSide) String() string {
	var buffer bytes.Buffer

	buffer.WriteString("prices:")
	for _, o := range os.prices {
		buffer.WriteString(
			fmt.Sprintf(
				"`%d` orders with price: `%s` with amount: `%s`.",
				o.orders.Len(),
				o.price.String(),
				o.totalAmount.String(),
			),
		)
	}

	buffer.WriteString("priceTree:")
	buffer.WriteString(os.priceTree.String())

	return buffer.String()
}

// ExecuteOrder executes order.
func (os *OrderSide) ExecuteOrder(o *Order) (amountLeft *apd.Decimal, ordersExecuted int, err error) {
	// how much amount we should find
	amountLeft = apd.New(0, 0)
	amountLeft.Set(o.amount)

	var found bool
	iter := os.priceTree.Iterator()

	if os.sideType == Ask { // there are sells here, let's find the min one
		found = iter.First()
	} else { // there are buys here, let's find the max one
		found = iter.Last()
	}

	if !found {
		return
	}

	i := 0
	deleteTreeNodes := make([]interface{}, 0)
	for {
		// we found required amount - it's done
		if amountLeft.IsZero() {
			break
		}

		// let's iterate by the tree to find required orders.
		if i != 0 {
			if os.sideType == Ask {
				found = iter.Next()
			} else {
				found = iter.Prev()
			}
			if !found { // we checked all nodes - there are no more.
				break
			}
		}

		orders := iter.Value().(*OrdersBySpecificPrice)

		// let's check if our price fits with the price from order book.
		res := o.price.Cmp(orders.price)
		if os.sideType == Ask {
			if res < 0 { // order price < orders.price
				break
			}
		} else {
			if res > 0 { // order price > orders.price
				break
			}
		}

		var treeNodeIsEmpty bool
		// we have some orders with amount, let's match it
		el := orders.orders.Front()
		deleteEls := make([]*list.Element, 0)
		for {
			var listNodeEmpty bool
			amountExecuted := apd.New(0, 0)
			amountFound := apd.New(0, 0)

			reqOrder := el.Value.(*Order)
			oe := ExecutionReport{
				initiatorOrderID: o.orderID,
				executorOrderID:  reqOrder.orderID,
				price:            reqOrder.price,
			}

			switch reqOrder.amount.Cmp(amountLeft) {
			case -1: // reqOrder.amount < amountLeft - we found only the small part, let's take it and look more
				amountFound = reqOrder.amount
				listNodeEmpty = true

			case 0: // reqOrder.amount == amountLeft - we found the exactly required amount
				amountFound = reqOrder.amount
				listNodeEmpty = true

			case 1: // reqOrder.amount > amountLeft - we found more than we need - let's take only required amount
				amountFound = amountLeft
			}

			oe.amount = amountFound
			amountExecuted = amountExecuted.Set(amountFound)

			if !listNodeEmpty {
				_, err = apd.BaseContext.Sub(reqOrder.amount, reqOrder.amount, amountFound)
				if err != nil {
					return nil, ordersExecuted, err
				}
			}

			_, err = apd.BaseContext.Sub(amountLeft, amountLeft, amountFound)
			if err != nil {
				return nil, ordersExecuted, err
			}

			if len(o.executions) == 0 {
				o.executions = make([]*ExecutionReport, 0)
			}
			o.executions = append(o.executions, &oe)

			if len(reqOrder.executions) == 0 {
				reqOrder.executions = make([]*ExecutionReport, 0)
			}
			reqOrder.executions = append(reqOrder.executions, &oe)

			ordersExecuted++

			if listNodeEmpty {
				deleteEls = append(deleteEls, el)
			}

			// recalc the total amount for that price
			_, err = apd.BaseContext.Sub(orders.totalAmount, orders.totalAmount, amountExecuted)
			if err != nil {
				return nil, ordersExecuted, err
			}

			// we found required amount - it's done
			if amountLeft.IsZero() {
				break
			}

			el = el.Next()
			if el == nil {
				// we checked all data in linked list - so the node of the tree is empty
				treeNodeIsEmpty = true
				break
			}
		}

		for _, delEl := range deleteEls {
			orders.orders.Remove(delEl)
			if orders.orders.Len() == 0 {
				delete(os.prices, orders.price.String())
			}
		}

		if treeNodeIsEmpty {
			deleteTreeNodes = append(deleteTreeNodes, iter.Key())
		}

		i++
	}

	for _, node := range deleteTreeNodes {
		os.priceTree.Remove(node)
	}

	return
}

// AddOrder adds order to the list side.
func (os *OrderSide) AddOrder(ctx context.Context, o *Order) (*list.Element, error) {
	// check that we have some orders on this price level
	priceS := o.price.String()
	ordersByPrice, ok := os.prices[priceS]
	if ok {
		// let's add more volume.
		err := ordersByPrice.AddAmount(o.amount)
		if err != nil {
			return nil, err
		}
	}
	if !ok {
		// there are no orders on this price level - let's create them.
		ordersByPrice = NewOrdersBySpecificPrice(o.price, o.amount)
		os.prices[priceS] = ordersByPrice
		os.priceTree.Put(o.price, ordersByPrice)
	}

	// let's add order to the list.
	return ordersByPrice.orders.PushBack(o), nil
}

// PlaceMarketOrder places a market order in OrderBook.
func (ob *OrderBook) PlaceMarketOrder(
	ctx context.Context, o *Order,
) (ordersExecuted int, amountLeft *apd.Decimal, err error) {
	// TODO: check that amount is valid.

	ob.mx.Lock()
	defer func() {
		if err == nil {
			ob.OrdersDone[o.orderID] = o
		}

		ob.mx.Unlock()
	}()

	var (
		sideToCheck *OrderSide
	)
	if o.operationType == Ask {
		sideToCheck = ob.Bids
	} else {
		sideToCheck = ob.Asks
	}

	amountLeft, ordersExecuted, err = sideToCheck.ExecuteOrder(o)
	if err != nil {
		return ordersExecuted, nil, fmt.Errorf("can't find about for order: %w", err)
	}

	// these orders were executed - delete them
	if o.executions != nil {
		for _, execution := range o.executions {
			delete(ob.Orders, execution.executorOrderID)
		}
	}

	return ordersExecuted, amountLeft, nil
}

// PlaceLimitOrder places a limit order in OrderBook.
func (ob *OrderBook) PlaceLimitOrder(ctx context.Context, o *Order) (ordersExecuted int, err error) {
	// TODO: check that amount and price are valid.
	ob.mx.Lock()
	defer ob.mx.Unlock()

	return ob.limitOrder(ctx, o)
}

func (ob *OrderBook) limitOrder(ctx context.Context, o *Order) (ordersExecuted int, err error) {
	defer func() {
		if err == nil {
			ob.OrdersDone[o.orderID] = o
		}
	}()

	_, ok := ob.Orders[o.orderID]
	if ok {
		return 0, fmt.Errorf("order: %s already exists", o.orderID)
	}

	var (
		sideToAdd, sideToCheck *OrderSide
	)
	if o.operationType == Ask {
		sideToAdd = ob.Asks
		sideToCheck = ob.Bids
	} else {
		sideToAdd = ob.Bids
		sideToCheck = ob.Asks
	}

	amountLeft, ordersExecuted, err := sideToCheck.ExecuteOrder(o)
	if err != nil {
		return ordersExecuted, fmt.Errorf("can't find about for order: %w", err)
	}

	// these orders were executed - delete them
	if o.executions != nil {
		for _, execution := range o.executions {
			delete(ob.Orders, execution.executorOrderID)
		}
	}

	if amountLeft.IsZero() {
		return ordersExecuted, nil
	}
	if ordersExecuted > 0 {
		o.amount = amountLeft
	}

	orderInList, err := sideToAdd.AddOrder(ctx, o)
	if err != nil {
		return ordersExecuted, fmt.Errorf("can't place limit order: %w", err)
	}
	ob.Orders[o.orderID] = orderInList

	return ordersExecuted, nil
}

// Rollback rollbacks the order by id.
// NOTE: This operation isn't atomic!!!!
func (ob *OrderBook) Rollback(ctx context.Context, orderID OrderID) error {
	ob.mx.Lock()
	defer ob.mx.Unlock()

	order, ok := ob.OrdersDone[orderID]
	if !ok {
		return fmt.Errorf("order: %s not found - nothing to rollback", orderID)
	}

	var operationTypeWas OperationType
	if order.operationType == Ask {
		operationTypeWas = Bid
	} else {
		operationTypeWas = Ask
	}

	for _, oe := range order.executions {
		_, err := ob.limitOrder(ctx, &Order{
			orderID:       oe.executorOrderID,
			operationType: operationTypeWas,
			amount:        oe.amount,
			price:         oe.price,
		})
		if err != nil {
			return fmt.Errorf("error in rollback: %w", err)
		}
	}

	delete(ob.OrdersDone, orderID)

	return nil
}
