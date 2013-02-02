package worker

import (
	"fmt"
	"github.com/msgbox/queue"
	"github.com/streadway/amqp"
	"log"
	"net"
)

type Relay struct {
	Addr string
}

// An Outgoing Worker should listen for new items on the outgoing
// queue and process them as necessary

func Outgoing(tag string, connection *amqp.Connection, relay *Relay) {

	// Create a Consumer
	c, err := queue.BuildConsumer(tag, connection)
	if err != nil {
		fmt.Errorf("Consumer Error: %s", err)
	}

	// Start the Worker
	deliveries, err := c.Channel.Consume(
		"outgoing_messages", // name
		c.Tag,               // consumerTag,
		true,                // noAck
		false,               // exclusive
		false,               // noLocal
		false,               // noWait
		nil,                 // arguments
	)
	if err != nil {
		fmt.Errorf("Queue Consume: %s", err)
	}

	go handleOutgoing(deliveries, c.Done, relay)

}

// Handle Outgoing messages from the queue
func handleOutgoing(deliveries <-chan amqp.Delivery, done chan error, relay *Relay) {
	for d := range deliveries {
		transfer(d.Body, relay)
		d.Ack(true)
	}

	done <- nil
}

func transfer(msg []byte, relay *Relay) {
	node := relay.Addr

	conn, err := net.Dial("tcp", node)
	if err != nil {
		// Handler Err
	}
	defer conn.Close()

	status, err := conn.Write(msg)
	if err != nil {
		// Handle Write Err
	}

	log.Printf("status = %s", status)

}
