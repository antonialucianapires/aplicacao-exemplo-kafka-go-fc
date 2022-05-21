package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)


func main() {

	configMap := &kafka.ConfigMap {
		"bootstrap.servers": "aplicacao-exemplo-kafka-go-fc-kafka-1:9092",
		"client.id": "goapp-consumer",
		"group.id": "goapp-group",
		"auto.offset.reset": "earliest", //le todas as mensagens produzidas
	}

	c, err := kafka.NewConsumer(configMap)
	if err != nil {
		fmt.Println("Erro ao consumir mensagem", err.Error())
	}

	topics := []string{"gokafka"}
	c.SubscribeTopics(topics, nil)

	for {
		msg, err := c.ReadMessage(-1)
		if err == nil {
			fmt.Println(string(msg.Value), msg.TopicPartition)
		}
	}

}