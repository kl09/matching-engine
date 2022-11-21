package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/apd"
)

func testOrders() []*Order {
	/*

		ASK:
			 2 BTC 20150 USDT
			 1 BTC (0.3+0.5+0.2) 20100 USDT
			 1 BTC (0.3+0.5+0.2) 20050 USDT

		--- here we place our orders ---

		BID:
			1 BTC (0.3+0.5+0.2) 20000 USDT
			1 BTC (0.3+0.5+0.2) 19900 USDT
			2 BTC 19850 USDT

	*/

	return []*Order{
		{
			orderID:       "1",
			operationType: Ask,
			amount:        apd.New(3, -1),
			price:         apd.New(20050, 0),
			createdAt:     time.Now(),
		},
		{
			orderID:       "11",
			operationType: Ask,
			amount:        apd.New(5, -1),
			price:         apd.New(20050, 0),
			createdAt:     time.Now(),
		},
		{
			orderID:       "111",
			operationType: Ask,
			amount:        apd.New(2, -1),
			price:         apd.New(20050, 0),
			createdAt:     time.Now(),
		},
		{
			orderID:       "2",
			operationType: Ask,
			amount:        apd.New(3, -1),
			price:         apd.New(20100, 0),
			createdAt:     time.Now(),
		},
		{
			orderID:       "22",
			operationType: Ask,
			amount:        apd.New(5, -1),
			price:         apd.New(20100, 0),
			createdAt:     time.Now(),
		},
		{
			orderID:       "222",
			operationType: Ask,
			amount:        apd.New(2, -1),
			price:         apd.New(20100, 0),
			createdAt:     time.Now(),
		},
		{
			orderID:       "3",
			operationType: Ask,
			amount:        apd.New(2, 0),
			price:         apd.New(20150, 0),
			createdAt:     time.Now(),
		},
		{
			orderID:       "4",
			operationType: Bid,
			amount:        apd.New(3, -1),
			price:         apd.New(20000, 0),
			createdAt:     time.Now(),
		},
		{
			orderID:       "44",
			operationType: Bid,
			amount:        apd.New(5, -1),
			price:         apd.New(20000, 0),
			createdAt:     time.Now(),
		},
		{
			orderID:       "444",
			operationType: Bid,
			amount:        apd.New(2, -1),
			price:         apd.New(20000, 0),
			createdAt:     time.Now(),
		},
		{
			orderID:       "5",
			operationType: Bid,
			amount:        apd.New(3, -1),
			price:         apd.New(19900, 0),
			createdAt:     time.Now(),
		},
		{
			orderID:       "55",
			operationType: Bid,
			amount:        apd.New(5, -1),
			price:         apd.New(19900, 0),
			createdAt:     time.Now(),
		},
		{
			orderID:       "555",
			operationType: Bid,
			amount:        apd.New(2, -1),
			price:         apd.New(19900, 0),
			createdAt:     time.Now(),
		},
		{
			orderID:       "6",
			operationType: Bid,
			amount:        apd.New(2, 0),
			price:         apd.New(19850, 0),
			createdAt:     time.Now(),
		},
	}
}

// Test_PlaceLimitOrder checks how we do place orders.
func Test_PlaceLimitOrder(t *testing.T) {
	orders := []*Order{
		{
			orderID:       "1",
			operationType: Ask,
			amount:        apd.New(1, -1),
			price:         apd.New(20100, 0),
			createdAt:     time.Now(),
		},
		{
			orderID:       "11",
			operationType: Ask,
			amount:        apd.New(1, -2),
			price:         apd.New(20100, 0),
			createdAt:     time.Now(),
		},
		{
			orderID:       "2",
			operationType: Ask,
			amount:        apd.New(1, -2),
			price:         apd.New(20110, 0),
			createdAt:     time.Now(),
		},
		{
			orderID:       "22",
			operationType: Ask,
			amount:        apd.New(1, -3),
			price:         apd.New(20110, 0),
			createdAt:     time.Now(),
		},
		{
			orderID:       "3",
			operationType: Ask,
			amount:        apd.New(1, -3),
			price:         apd.New(20120, 0),
			createdAt:     time.Now(),
		},
		{
			orderID:       "1000",
			operationType: Bid,
			amount:        apd.New(1, -1),
			price:         apd.New(20000, 0),
			createdAt:     time.Now(),
		},
		{
			orderID:       "1011",
			operationType: Bid,
			amount:        apd.New(1, -2),
			price:         apd.New(20000, 0),
			createdAt:     time.Now(),
		},
		{
			orderID:       "1001",
			operationType: Bid,
			amount:        apd.New(1, -2),
			price:         apd.New(19900, 0),
			createdAt:     time.Now(),
		},
		{
			orderID:       "1003",
			operationType: Bid,
			amount:        apd.New(1, -3),
			price:         apd.New(19800, 0),
			createdAt:     time.Now(),
		},
	}

	ob := NewOrderBook("BTC", "USDT")

	for _, o := range orders {
		ordersExecuted, err := ob.PlaceLimitOrder(context.Background(), o)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if ordersExecuted != 0 {
			t.Fatalf("expected 0 ordersExecuted, but got: %d", ordersExecuted)
		}
	}

	asksExpected := []string{
		"`2` orders with price: `20100` with amount: `0.11`",
		"`2` orders with price: `20110` with amount: `0.011`",
		"`1` orders with price: `20120` with amount: `0.001`",
	}

	for _, s := range asksExpected {
		if !strings.Contains(ob.Asks.String(), s) {
			t.Fatalf("didn't get required strings in asks : %s", s)
		}
	}

	bidsExpected := []string{
		"`2` orders with price: `20000` with amount: `0.11`",
		"`1` orders with price: `19900` with amount: `0.01`",
		"`1` orders with price: `19800` with amount: `0.001`",
	}

	for _, s := range bidsExpected {
		if !strings.Contains(ob.Bids.String(), s) {
			t.Fatalf("didn't get required strings in bids: %s", s)
		}
	}

	if len(ob.Orders) != len(orders) {
		t.Fatalf("orders amount is wrong")
	}
}

func Test_LimitOrderExecution(t *testing.T) {
	testcases := []struct {
		testName               string
		ordersToPlace          []*Order
		ordersExecutedExpected int
		expectedAskData        []string
		expectedBidData        []string
	}{
		{
			// there are no asks for this price in the order book - so we expect the creation of new order.
			testName: "buy order for 0.1 with price 20020",
			ordersToPlace: []*Order{
				{
					orderID:       "100500",
					operationType: Bid,
					amount:        apd.New(1, -1),
					price:         apd.New(20020, 0),
				},
			},
			ordersExecutedExpected: 0,
			expectedBidData: []string{
				"`1` orders with price: `20020` with amount: `0.1`",
			},
		},
		{
			testName: "buy order for 0.1 with price 20050",
			ordersToPlace: []*Order{
				{
					orderID:       "100500",
					operationType: Bid,
					amount:        apd.New(1, -1),
					price:         apd.New(20050, 0),
				},
			},
			ordersExecutedExpected: 1,
			expectedAskData: []string{
				// we still have 3 orders, but amount was decreased
				"`3` orders with price: `20050` with amount: `0.9`",
			},
		},
		{
			testName: "buy order for 0.3 with price 20050",
			ordersToPlace: []*Order{
				{
					orderID:       "100500",
					operationType: Bid,
					amount:        apd.New(3, -1),
					price:         apd.New(20050, 0),
				},
			},
			ordersExecutedExpected: 1,
			expectedAskData: []string{
				"`2` orders with price: `20050` with amount: `0.7`",
			},
		},
		{
			testName: "buy order for 0.4 with price 20050",
			ordersToPlace: []*Order{
				{
					orderID:       "100500",
					operationType: Bid,
					amount:        apd.New(4, -1),
					price:         apd.New(20050, 0),
				},
			},
			ordersExecutedExpected: 2,
			expectedAskData: []string{
				"`2` orders with price: `20050` with amount: `0.6`",
			},
		},
		{
			testName: "buy order for 0.8 with price 20050",
			ordersToPlace: []*Order{
				{
					orderID:       "100500",
					operationType: Bid,
					amount:        apd.New(8, -1),
					price:         apd.New(20050, 0),
				},
			},
			ordersExecutedExpected: 2,
			expectedAskData: []string{
				"`1` orders with price: `20050` with amount: `0.2`",
			},
		},
		{

			testName: "buy order for 1.5 with price 20150",
			ordersToPlace: []*Order{
				{
					orderID:       "100500",
					operationType: Bid,
					amount:        apd.New(15, -1),
					price:         apd.New(20150, 0),
				},
			},
			ordersExecutedExpected: 5,
			expectedAskData: []string{
				"`2` orders with price: `20100` with amount: `0.5`",
			},
		},
		{

			testName: "buy order for 2.5 with price 20150",
			ordersToPlace: []*Order{
				{
					orderID:       "100500",
					operationType: Bid,
					amount:        apd.New(25, -1),
					price:         apd.New(20150, 0),
				},
			},
			ordersExecutedExpected: 7,
			expectedAskData: []string{
				"`1` orders with price: `20150` with amount: `1.0`",
			},
		},
		{

			testName: "buy order for 100 with price 22000",
			ordersToPlace: []*Order{
				{
					orderID:       "100500",
					operationType: Bid,
					amount:        apd.New(100, 0),
					price:         apd.New(22000, 0),
				},
			},
			ordersExecutedExpected: 7,
			expectedAskData: []string{
				// no asks in the order book
				"",
			},
			expectedBidData: []string{
				"`1` orders with price: `22000` with amount: `96.0`",
			},
		},
		{
			// there are no bids for this price in the order book - so we expect the creation of new order.
			testName: "sell order for 0.1 with price 20020",
			ordersToPlace: []*Order{
				{
					orderID:       "100500",
					operationType: Ask,
					amount:        apd.New(1, -1),
					price:         apd.New(20020, 0),
				},
			},
			ordersExecutedExpected: 0,
			expectedAskData: []string{
				"`1` orders with price: `20020` with amount: `0.1`",
			},
		},
		{
			testName: "sell order for 0.1 with price 20000",
			ordersToPlace: []*Order{
				{
					orderID:       "100500",
					operationType: Ask,
					amount:        apd.New(1, -1),
					price:         apd.New(20000, 0),
				},
			},
			ordersExecutedExpected: 1,
			expectedBidData: []string{
				// we still have 3 orders, but amount was decreased
				"`3` orders with price: `20000` with amount: `0.9`",
			},
		},
		{
			testName: "sell order for 0.3 with price 20000",
			ordersToPlace: []*Order{
				{
					orderID:       "100500",
					operationType: Ask,
					amount:        apd.New(3, -1),
					price:         apd.New(20000, 0),
				},
			},
			ordersExecutedExpected: 1,
			expectedBidData: []string{
				"`2` orders with price: `20000` with amount: `0.7`",
			},
		},
		{
			testName: "sell order for 0.4 with price 20000",
			ordersToPlace: []*Order{
				{
					orderID:       "100500",
					operationType: Ask,
					amount:        apd.New(4, -1),
					price:         apd.New(20000, 0),
				},
			},
			ordersExecutedExpected: 2,
			expectedBidData: []string{
				"`2` orders with price: `20000` with amount: `0.6`",
			},
		},
		{
			testName: "sell order for 0.8 with price 20000",
			ordersToPlace: []*Order{
				{
					orderID:       "100500",
					operationType: Ask,
					amount:        apd.New(8, -1),
					price:         apd.New(20000, 0),
				},
			},
			ordersExecutedExpected: 2,
			expectedBidData: []string{
				"`1` orders with price: `20000` with amount: `0.2`",
			},
		},
		{

			testName: "sell order for 1.5 with price 19900",
			ordersToPlace: []*Order{
				{
					orderID:       "100500",
					operationType: Ask,
					amount:        apd.New(15, -1),
					price:         apd.New(19900, 0),
				},
			},
			ordersExecutedExpected: 5,
			expectedBidData: []string{
				"`2` orders with price: `19900` with amount: `0.5`",
			},
		},
		{

			testName: "sell order for 2.5 with price 19850",
			ordersToPlace: []*Order{
				{
					orderID:       "100500",
					operationType: Ask,
					amount:        apd.New(25, -1),
					price:         apd.New(19850, 0),
				},
			},
			ordersExecutedExpected: 7,
			expectedBidData: []string{
				"`1` orders with price: `19850` with amount: `1.0`",
			},
		},
		{

			testName: "sell order for 100 with price 19000",
			ordersToPlace: []*Order{
				{
					orderID:       "100500",
					operationType: Ask,
					amount:        apd.New(100, 0),
					price:         apd.New(19000, 0),
				},
			},
			ordersExecutedExpected: 7,
			expectedBidData: []string{
				// no asks in the order book
				"",
			},
			expectedAskData: []string{
				"`1` orders with price: `19000` with amount: `96.0`",
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.testName, func(t *testing.T) {
			ob := NewOrderBook("BTC", "USDT")
			for _, o := range testOrders() {
				_, err := ob.PlaceLimitOrder(context.Background(), o)
				if err != nil {
					t.Fatalf("unexpected err: %v", err)
				}
			}

			ordersExecuted := 0
			for _, o := range tc.ordersToPlace {
				oe, err := ob.PlaceLimitOrder(context.Background(), o)
				if err != nil {
					t.Fatalf("unexpected err: %v", err)
				}

				ordersExecuted += oe
			}

			if ordersExecuted != tc.ordersExecutedExpected {
				t.Fatalf("expected %d ordersExecuted, but got: %d", tc.ordersExecutedExpected, ordersExecuted)
			}

			for _, s := range tc.expectedAskData {
				if !strings.Contains(ob.Asks.String(), s) {
					t.Fatalf("didn't get required strings in bids: %s", s)
				}
			}

			for _, s := range tc.expectedBidData {
				if !strings.Contains(ob.Bids.String(), s) {
					t.Fatalf("didn't get required strings in bids: %s", s)
				}
			}
		})
	}
}

func Test_MarketOrderExecution(t *testing.T) {
	testcases := []struct {
		testName               string
		orderToPlace           *Order
		ordersExecutedExpected int
		expectedAskData        []string
		expectedBidData        []string
		amountLeftExpected     string
	}{
		{
			// there are no asks for this price in the order book - so we expect the creation of new order.
			testName: "buy order for 0.1 with price 20020",
			orderToPlace: &Order{
				orderID:       "100500",
				operationType: Bid,
				amount:        apd.New(1, -1),
				price:         apd.New(20020, 0),
			},
			ordersExecutedExpected: 0,
			amountLeftExpected:     "0.1",
		},
		{
			testName: "buy order for 0.1 with price 20050",
			orderToPlace: &Order{
				orderID:       "100500",
				operationType: Bid,
				amount:        apd.New(1, -1),
				price:         apd.New(20050, 0),
			},
			ordersExecutedExpected: 1,
			expectedAskData: []string{
				// we still have 3 orders, but amount was decreased
				"`3` orders with price: `20050` with amount: `0.9`",
			},
			amountLeftExpected: "0.0",
		},
		{
			testName: "buy order for 0.3 with price 20050",
			orderToPlace: &Order{
				orderID:       "100500",
				operationType: Bid,
				amount:        apd.New(3, -1),
				price:         apd.New(20050, 0),
			},
			ordersExecutedExpected: 1,
			expectedAskData: []string{
				"`2` orders with price: `20050` with amount: `0.7`",
			},
			amountLeftExpected: "0.0",
		},
		{
			testName: "buy order for 0.4 with price 20050",
			orderToPlace: &Order{
				orderID:       "100500",
				operationType: Bid,
				amount:        apd.New(4, -1),
				price:         apd.New(20050, 0),
			},
			ordersExecutedExpected: 2,
			expectedAskData: []string{
				"`2` orders with price: `20050` with amount: `0.6`",
			},
			amountLeftExpected: "0.0",
		},
		{
			testName: "buy order for 0.8 with price 20050",
			orderToPlace: &Order{
				orderID:       "100500",
				operationType: Bid,
				amount:        apd.New(8, -1),
				price:         apd.New(20050, 0),
			},
			ordersExecutedExpected: 2,
			expectedAskData: []string{
				"`1` orders with price: `20050` with amount: `0.2`",
			},
			amountLeftExpected: "0.0",
		},
		{

			testName: "buy order for 1.5 with price 20150",
			orderToPlace: &Order{
				orderID:       "100500",
				operationType: Bid,
				amount:        apd.New(15, -1),
				price:         apd.New(20150, 0),
			},
			ordersExecutedExpected: 5,
			expectedAskData: []string{
				"`2` orders with price: `20100` with amount: `0.5`",
			},
			amountLeftExpected: "0.0",
		},
		{

			testName: "buy order for 2.5 with price 20150",
			orderToPlace: &Order{
				orderID:       "100500",
				operationType: Bid,
				amount:        apd.New(25, -1),
				price:         apd.New(20150, 0),
			},
			ordersExecutedExpected: 7,
			expectedAskData: []string{
				"`1` orders with price: `20150` with amount: `1.0`",
			},
			amountLeftExpected: "0.0",
		},
		{

			testName: "buy order for 100 with price 22000",
			orderToPlace: &Order{
				orderID:       "100500",
				operationType: Bid,
				amount:        apd.New(100, 0),
				price:         apd.New(22000, 0),
			},
			ordersExecutedExpected: 7,
			expectedAskData: []string{
				// no asks in the order book
				"",
			},
			amountLeftExpected: "96.0",
		},
		{
			// there are no bids for this price in the order book - so we expect the creation of new order.
			testName: "sell order for 0.1 with price 20020",
			orderToPlace: &Order{
				orderID:       "100500",
				operationType: Ask,
				amount:        apd.New(1, -1),
				price:         apd.New(20020, 0),
			},
			ordersExecutedExpected: 0,
			amountLeftExpected:     "0.1",
		},
		{
			testName: "sell order for 0.1 with price 20000",
			orderToPlace: &Order{
				orderID:       "100500",
				operationType: Ask,
				amount:        apd.New(1, -1),
				price:         apd.New(20000, 0),
			},
			ordersExecutedExpected: 1,
			expectedBidData: []string{
				// we still have 3 orders, but amount was decreased
				"`3` orders with price: `20000` with amount: `0.9`",
			},
			amountLeftExpected: "0.0",
		},
		{
			testName: "sell order for 0.3 with price 20000",
			orderToPlace: &Order{
				orderID:       "100500",
				operationType: Ask,
				amount:        apd.New(3, -1),
				price:         apd.New(20000, 0),
			},
			ordersExecutedExpected: 1,
			expectedBidData: []string{
				"`2` orders with price: `20000` with amount: `0.7`",
			},
			amountLeftExpected: "0.0",
		},
		{
			testName: "sell order for 0.4 with price 20000",
			orderToPlace: &Order{
				orderID:       "100500",
				operationType: Ask,
				amount:        apd.New(4, -1),
				price:         apd.New(20000, 0),
			},
			ordersExecutedExpected: 2,
			expectedBidData: []string{
				"`2` orders with price: `20000` with amount: `0.6`",
			},
			amountLeftExpected: "0.0",
		},
		{
			testName: "sell order for 0.8 with price 20000",
			orderToPlace: &Order{
				orderID:       "100500",
				operationType: Ask,
				amount:        apd.New(8, -1),
				price:         apd.New(20000, 0),
			},
			ordersExecutedExpected: 2,
			expectedBidData: []string{
				"`1` orders with price: `20000` with amount: `0.2`",
			},
			amountLeftExpected: "0.0",
		},
		{

			testName: "sell order for 1.5 with price 19900",
			orderToPlace: &Order{
				orderID:       "100500",
				operationType: Ask,
				amount:        apd.New(15, -1),
				price:         apd.New(19900, 0),
			},
			ordersExecutedExpected: 5,
			expectedBidData: []string{
				"`2` orders with price: `19900` with amount: `0.5`",
			},
			amountLeftExpected: "0.0",
		},
		{

			testName: "sell order for 2.5 with price 19850",
			orderToPlace: &Order{
				orderID:       "100500",
				operationType: Ask,
				amount:        apd.New(25, -1),
				price:         apd.New(19850, 0),
			},
			ordersExecutedExpected: 7,
			expectedBidData: []string{
				"`1` orders with price: `19850` with amount: `1.0`",
			},
			amountLeftExpected: "0.0",
		},
		{

			testName: "sell order for 100 with price 19000",
			orderToPlace: &Order{
				orderID:       "100500",
				operationType: Ask,
				amount:        apd.New(100, 0),
				price:         apd.New(19000, 0),
			},
			ordersExecutedExpected: 7,
			expectedBidData: []string{
				// no asks in the order book
				"",
			},
			amountLeftExpected: "96.0",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.testName, func(t *testing.T) {
			ob := NewOrderBook("BTC", "USDT")
			for _, o := range testOrders() {
				_, err := ob.PlaceLimitOrder(context.Background(), o)
				if err != nil {
					t.Fatalf("unexpected err: %v", err)
				}
			}

			ordersExecuted, amountLeft, err := ob.PlaceMarketOrder(context.Background(), tc.orderToPlace)
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}

			if ordersExecuted != tc.ordersExecutedExpected {
				t.Fatalf("expected %d ordersExecuted, but got: %d", tc.ordersExecutedExpected, ordersExecuted)
			}

			if amountLeft.String() != tc.amountLeftExpected {
				t.Fatalf("expected %s amount, but got: %s", tc.amountLeftExpected, amountLeft.String())
			}

			for _, s := range tc.expectedAskData {
				if !strings.Contains(ob.Asks.String(), s) {
					t.Fatalf("didn't get required strings in bids: %s", s)
				}
			}

			for _, s := range tc.expectedBidData {
				if !strings.Contains(ob.Bids.String(), s) {
					t.Fatalf("didn't get required strings in bids: %s", s)
				}
			}
		})
	}
}

func Test_OrderBookRollback(t *testing.T) {
	order := &Order{
		orderID:       "100500",
		operationType: Bid,
		amount:        apd.New(2, 0),
		price:         apd.New(25000, 0),
	}

	ob := NewOrderBook("BTC", "USDT")
	for _, o := range testOrders() {
		_, err := ob.PlaceLimitOrder(context.Background(), o)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
	}

	logBeforeOrder := ob.Asks.String()

	_, err := ob.PlaceLimitOrder(context.Background(), order)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	err = ob.Rollback(context.Background(), "991122")
	if err == nil {
		t.Fatalf("expected err, but got nil")
	}

	if err.Error() != "order: 991122 not found - nothing to rollback" {
		t.Fatalf("unexpected err")
	}

	err = ob.Rollback(context.Background(), order.orderID)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if len(logBeforeOrder) != len(ob.Asks.String()) {
		t.Fatalf("log before doesn't equal to log after")
	}

	// let's try to rollback again - we should get an error.
	err = ob.Rollback(context.Background(), order.orderID)
	if err == nil {
		t.Fatalf("expected err, but got nil")
	}

	if err.Error() != "order: 100500 not found - nothing to rollback" {
		t.Fatalf("unexpected err")
	}
}
