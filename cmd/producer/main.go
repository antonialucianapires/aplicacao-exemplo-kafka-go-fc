package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
)


func main() {
	producer := NewKafkaProducer()
	Publish("ola gogo!", "gokafka", producer, nil)
	producer.Flush(1000) //O Flush espera um retorno ou um tempo de milisegundos para encerrar o main
	fmt.Println("Mensagem enviada!")
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

func Publish(msg string, topic string, producer *kafka.Producer, key []byte) error {
	message := &kafka.Message{
		Value:          []byte(msg),
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            key,
	}
	err := producer.Produce(message, nil)
	if err != nil {
		return err
	}
	return nil
}