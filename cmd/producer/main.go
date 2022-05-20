package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)


func main() {
	fmt.Println("Hello Go")
}

func newKafkaProducer() *kafka.Producer {
	configMap := &kafka.ConfigMap {
		"bootstrap.servers": "aplicacao-exemplo-kafka-go-fc-kafka-1:9092"
	}

	p, err := kafka.newProducer(configMap)

	if err != nil {
		log.Println(err.Error())
	}

	return p

}