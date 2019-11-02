package main

import (
	"fmt"
	"os"
	"os/signal"

	kClient "github.com/Shopify/sarama"
)

func main() {
	config := kClient.NewConfig()
	config.Consumer.Return.Errors = true

	brokers := []string{"localhost:9092"}
	// topics := []string{"message"}
	consumer, err := kClient.NewConsumer(brokers, nil)

	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition("message", 0, kClient.OffsetNewest)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	for {
		select {
		case msg, more := <-partitionConsumer.Messages():
			if more {
				fmt.Fprintf(os.Stdout, "Message Partition is [%d] and current offset is [%d] > %s", msg.Partition, msg.Offset, msg.Value)
			}
		case <-signals:
			break
		}
	}

}
