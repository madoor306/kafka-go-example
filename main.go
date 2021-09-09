package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

// Topic and Broker Addresses are intitialized as constants
const (
	topic          = "message-log"
	broker1Address = "localhost:9093"
	broker2Address = "localhost:9094"
	broker3Address = "localhost:9095"
)

//Producer for the Apache kafka topic
func produceMessage(ctx context.Context) {
	// Initialize a counter variable
	i := 0

	l := log.New(os.Stdout, "Kafka Writer:", 0)

	// Initialize the writer with broker address and the topic
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{broker1Address, broker2Address, broker3Address},
		Topic:   topic,
		Logger:  l,
	})

	for {
		// each kafka message has a key and value. The key is used
		// to decide which partition (and consequently, which broker)
		// the message gets published on
		err := writer.WriteMessages(ctx, kafka.Message{
			Key:   []byte(strconv.Itoa(i)),
			Value: []byte("This is a message from : " + strconv.Itoa(i)),
		})

		if err != nil {
			panic("Couldn't write message : " + err.Error())
		}

		// log a confirmation once the message is written
		fmt.Println("writes:", i)
		i++
		// sleep for a second
		time.Sleep(time.Second)
	}
}

//Consumer for the Apache kafka topic
func consumeMessage(ctx context.Context) {

	l := log.New(os.Stdout, "Kafka Writer:", 0)

	// initialize a new reader with the brokers and topic
	// the groupID identifies the consumer and prevents
	// it from receiving duplicate messages
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{broker1Address, broker2Address, broker3Address},
		Topic:    topic,
		Logger:   l,
		GroupID:  "my-group",
		MinBytes: 5,
		MaxBytes: 1e6,
		// Wait for at most 3 sec before receiving new data
		MaxWait: 3 * time.Second,
	})

	for {
		// the `ReadMessage` method blocks until we receive the next event
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			panic("could not read message " + err.Error())
		}
		// after receiving the message, log its value
		fmt.Println("received: ", string(msg.Value))
	}

}

func main() {
	fmt.Println("Starting event streaming example....")

	// create a context
	ctx := context.Background()
	go produceMessage(ctx)
	consumeMessage(ctx)
}
