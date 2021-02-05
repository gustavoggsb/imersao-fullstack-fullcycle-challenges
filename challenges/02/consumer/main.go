package main

import (
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/joho/godotenv"
)

func main() {
	godotenv.Load()
	consumer := newKafkaConsumer()

	fmt.Println("Consumer: Kafka consumer is waiting for message...")
	for {
		msg, err := consumer.ReadMessage(-1)
		if err == nil {
			fmt.Println("Consumer: Message received:")
			fmt.Println(string(msg.Value))
		}
	}

}

func newKafkaConsumer() *kafka.Consumer {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": os.Getenv("kafkaBootstrapServers"),
		"group.id":          os.Getenv("kafkaConsumerGroupId"),
		"auto.offset.reset": "earliest",
	}

	c, err := kafka.NewConsumer(configMap)

	if err != nil {
		panic(err)
	}

	topics := []string{os.Getenv("kafkaChallengeTopic")}
	c.SubscribeTopics(topics, nil)

	fmt.Println("Consumer: Kafka consumer has been started")

	return c
}

// func processMessage(msg *kafka.Message) {
// 	transactionsTopic := "transactions"
// 	transactionConfirmationTopic := "transaction_confirmation"

// 	switch topic := *msg.TopicPartition.Topic; topic {
// 	case transactionsTopic:
// 		k.processTransaction(msg)
// 	case transactionConfirmationTopic:
// 		k.processTransactionConfirmation(msg)
// 	default:
// 		fmt.Println("Not a valid topic", string(msg.Value))
// 	}
// }
