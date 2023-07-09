package main

import (
	"encoding/json"
	"exchangeplatform/backendexchange"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
)

func createConsumer(topic string) *cluster.Consumer {
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Offsets.CommitInterval = 1 * time.Second
	config.Consumer.MaxWaitTime = 30 * time.Second
	config.Consumer.MaxProcessingTime = 10 * time.Second

	brokers := []string{""}
	consumer, err := cluster.NewConsumer(brokers, "myconsumer", []string{topic}, config)
	if err != nil {
		log.Fatal("Unable to connect consumer to kafka cluster")
	}

	go handleErrors(consumer)
	go handleNotifications(consumer)

	return consumer
}

func createProducer() sarama.AsyncProducer {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = false
	config.Producer.Return.Errors = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Net.DialTimeout = 30 * time.Second
	config.Net.ReadTimeout = 30 * time.Second
	config.Net.WriteTimeout = 30 * time.Second

	brokers := []string{""}
	producer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		log.Fatal("Unable to connect producer to kafka server")
	}
	return producer
}

func handleErrors(consumer *cluster.Consumer) {
	for err := range consumer.Errors() {
		log.Printf("Error: %s\n", err.Error())
	}
}

func handleNotifications(consumer *cluster.Consumer) {
	for ntf := range consumer.Notifications() {
		log.Printf("Rebalanced: %+v\n", ntf)
	}
}

type Worker struct {
	id       int
	orderCh  chan backendexchange.Order
	wg       *sync.WaitGroup
	mu       *sync.Mutex
	book     *backendexchange.OrderBook
	producer sarama.AsyncProducer
}

func NewWorker(id int, orderCh chan backendexchange.Order, wg *sync.WaitGroup, mu *sync.Mutex, book *backendexchange.OrderBook, producer sarama.AsyncProducer) *Worker {
	return &Worker{
		id:       id,
		orderCh:  orderCh,
		wg:       wg,
		mu:       mu,
		book:     book,
		producer: producer,
	}
}

func (w *Worker) Start() {
	go func() {
		for order := range w.orderCh {
			trades := w.book.Process(order, w.mu)
			log.Printf("Worker %d processed order: %+v, generated trades: %+v", w.id, order, trades)
			for _, trade := range trades {
				tradeJSON, _ := json.Marshal(trade)
				msg := &sarama.ProducerMessage{
					Topic: "trades",
					Value: sarama.StringEncoder(tradeJSON),
				}
				w.producer.Input() <- msg
			}
			w.wg.Done()
		}
	}()
}

func main() {

	var wg sync.WaitGroup
	var mu sync.Mutex

	orderCh := make(chan backendexchange.Order)

	producer := createProducer()
	defer producer.Close()

	go func() {
		for err := range producer.Errors() {
			log.Printf("Failed to send message to broker: %v\n", err)
		}
	}()

	consumer := createConsumer("orders")
	defer consumer.Close()

	numWorkers := 4

	book := &backendexchange.OrderBook{
		BuyOrders:  make([]backendexchange.Order, 0, 10000000),
		SellOrders: make([]backendexchange.Order, 0, 10000000),
	}

	for i := 0; i < numWorkers; i++ {
		w := NewWorker(i, orderCh, &wg, &mu, book, producer)
		w.Start()
	}

	go func() {
		for msg := range consumer.Messages() {
			var order backendexchange.Order
			order.FromJSON(msg.Value)
			wg.Add(1)
			orderCh <- order
			consumer.MarkOffset(msg, "")
		}
	}()

	time.Sleep(1 * time.Second)

	numOrders := 100000
	sampleOrders := make([]backendexchange.Order, numOrders)
	for i := 0; i < numOrders; i++ {
		side := int8(rand.Intn(2) + 1)
		var price uint64
		if side == 1 {
			price = uint64(rand.Intn(251) + 250)
		} else {
			price = uint64(rand.Intn(251))
		}
		exchangeTypes := []string{"USD-CAD", "EUR-USD"}
		order := backendexchange.Order{
			Amount:       uint64(rand.Intn(1000)),
			Price:        price,
			ID:           fmt.Sprintf("order-%d", i),
			Side:         side,
			ExchangeType: exchangeTypes[rand.Intn(len(exchangeTypes))],
		}
		sampleOrders[i] = order
	}

	for _, order := range sampleOrders {
		orderJSON, _ := json.Marshal(order)
		msg := &sarama.ProducerMessage{
			Topic: "orders",
			Value: sarama.StringEncoder(orderJSON),
		}
		producer.Input() <- msg
	}

	start := time.Now()

	for _, order := range sampleOrders {
		wg.Add(1)
		orderCh <- order
	}

	wg.Wait()

	elapsed := time.Since(start)
	log.Printf("Processing %d orders took %s", len(sampleOrders), elapsed)

	time.Sleep(5 * time.Second)
}
