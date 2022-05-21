package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
)


func main() {
	deliveryChannel := make(chan kafka.Event)

	producer := NewKafkaProducer()

	Publish("pagamento realizado com sucesso", "gokafka", producer, []byte("pagamento"), deliveryChannel)
	go DeliveryReport(deliveryChannel)

	producer.Flush(1000)
	
}

func NewKafkaProducer() *kafka.Producer {
	configMap := &kafka.ConfigMap {
		"bootstrap.servers": "aplicacao-exemplo-kafka-go-fc-kafka-1:9092",
		"delivery.timeout.ms": "0", //tempo máximo de entrega de uma mensagem
		"acks": "all", //o acks ALL possui um tempo maior de processamento, ou seja, afeta a performance. Porém, confirma que o broker leader e seus followers receberam  a mensagem
		"enable.idempotence": "true", //garante que a memsagem será enviada uma vez e não haverá duplicidade - afeta a performance
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

func DeliveryReport(deliveryChannel chan kafka.Event) {
	for e := range deliveryChannel {
		switch ev := e.(type) {
		case *kafka.Message:

			if ev.TopicPartition.Error != nil {
				fmt.Println("Erro ao enviar mensagem")
			} else {
				fmt.Println("Mensagem enviada: ", ev.TopicPartition)
			}
			
		}
	}
}