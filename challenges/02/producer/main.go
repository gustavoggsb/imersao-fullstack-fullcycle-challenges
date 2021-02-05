package main

import (
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/joho/godotenv"
)

func main() {
	godotenv.Load()
	producer := newKafkaProducer()
	deliveryChan := make(chan kafka.Event)
	topic := os.Getenv("kafkaChallengeTopic")
	msg := "I'm a fullcycle developer!!!!"

	fmt.Println("Producer: Sending message to topic: " + topic)
	publish(msg, topic, producer, deliveryChan)
	deliveryReport(deliveryChan)
}

func newKafkaProducer() *kafka.Producer {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": os.Getenv("kafkaBootstrapServers"),
	}
	p, err := kafka.NewProducer(configMap)
	if err != nil {
		panic(err)
	}

	return p
}

func publish(msg string, topic string, producer *kafka.Producer, deliveryChan chan kafka.Event) error {
	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: []byte(msg),
	}

	err := producer.Produce(message, deliveryChan)
	if err != nil {
		return err
	}

	return nil
}

func deliveryReport(deliveryChan chan kafka.Event) {
	for e := range deliveryChan {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				fmt.Println("Producer: Delivery failed: ", ev.TopicPartition)
			} else {
				fmt.Println("Producer: Delivered message to: ", ev.TopicPartition)
			}
		}

	}
}
