package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"

	kClient "github.com/Shopify/sarama"
)

type Producer struct {
	MessageProducer kClient.SyncProducer
}

func NewProducer(brokers []string) *Producer {
	config := kClient.NewConfig()
	config.Producer.Partitioner = kClient.NewRandomPartitioner
	config.Producer.RequiredAcks = kClient.WaitForAll
	config.Producer.Return.Successes = true
	c, err := kClient.NewSyncProducer(brokers, config)

	if err != nil {
		panic(err)
	}

	return &Producer{MessageProducer: c}
}

func (p *Producer) Close() error {
	err := p.MessageProducer.Close()
	if err != nil {
		return err
	}
	return nil
}

func (p *Producer) SendStringData(message string, topic string) error {
	partition, offset, err := p.MessageProducer.SendMessage(&kClient.ProducerMessage{
		Topic: topic,
		Value: kClient.StringEncoder(message),
	})

	if err != nil {
		return err
	}

	fmt.Printf("%d %d \n", partition, offset)
	return nil
}

func main() {
	reader := bufio.NewReader(os.Stdin)
	brokerURLs := os.Getenv("kafka_url")
	brokers := strings.Split(brokerURLs, ",")
	kClient.Logger = log.New(os.Stdout, "[kafka client] ", log.LstdFlags)
	producer := NewProducer(brokers)

	for {
		fmt.Print("> ")
		message, _ := reader.ReadString('\n')
		re := regexp.MustCompile(`\r?\n`)
		message = re.ReplaceAllString(message, "")
		if message == "quit" || message == "exit" {
			break
		}
		producer.SendStringData(message, "message")
	}
}
