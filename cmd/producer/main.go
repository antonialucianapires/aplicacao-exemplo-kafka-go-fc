package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
)


func main() {
	deliveryChannel := make(chan kafka.Event)

	producer := NewKafkaProducer()
	Publish("ola gogo!", "gokafka", producer, nil, deliveryChannel)

	e := <-deliveryChannel
	msg := e.(*kafka.Message)

	// Recupera o retorno do envio da mensagem de forma síncrona
	if msg.TopicPartition.Error != nil {
		fmt.Println("Erro ao enviar mensagem")
	} else {
		fmt.Println("Mensagem enviada: ", msg.TopicPartition)
	}

	producer.Flush(1000)
	
}

func NewKafkaProducer() *kafka.Producer {
	configMap := &kafka.ConfigMap {
		"bootstrap.servers": "aplicacao-exemplo-kafka-go-fc-kafka-1:9092",
	}

	p, err := kafka.NewProducer(configMap)

	if err != nil {
		log.Println(err.Error())
	}

	return p

}

func Publish(msg string, topic string, producer *kafka.Producer, key []byte, deliveryChannel chan kafka.Event) error {
	message := &kafka.Message{
		Value:          []byte(msg),
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            key,
	}
	err := producer.Produce(message, deliveryChannel)
	if err != nil {
		return err
	}
	return nil
}