package main

import (
	"fmt"
	"github.com/IBM/sarama"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	topic := "comments"
	worker, err := connectConsumer([]string{"localhost:29092"})
	if err != nil {
		panic(err)
	}

	consumer, err := worker.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		panic(err)
	}

	fmt.Println("Sarama consumer up and running!...")

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	msgCount := 0

	doneChan := make(chan struct{})

	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				fmt.Println("Error:", err)
			case msg := <-consumer.Messages():
				msgCount++
				fmt.Printf("Consumed message %d from topic %s: %s\n", msgCount, msg.Topic, string(msg.Value))
			case <-sigchan:
				fmt.Println("Received shutdown signal, shutting down gracefully")
				doneChan <- struct{}{}
				return // Exit the goroutine
			}
		}
	}()

	<-doneChan
	fmt.Println("Sarama consumer shutting down gracefully")
	if err := worker.Close(); err != nil {
		panic(err)
	}
}

func connectConsumer(brokersUrl []string) (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	conn, err := sarama.NewConsumer(brokersUrl, config)
	if err != nil {
		return nil, err
	}
	return conn, nil
}
