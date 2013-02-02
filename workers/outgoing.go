package worker

import (
	"fmt"
	"github.com/msgbox/queue"
	"github.com/streadway/amqp"
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
	checkError(err)

	// Start the Consuming
	deliveries, err := c.Channel.Consume(
		"outgoing_messages", // name
		c.Tag,               // consumerTag,
		true,                // noAck
		false,               // exclusive
		false,               // noLocal
		false,               // noWait
		nil,                 // arguments
	)
	checkError(err)

	// Handle messages coming off the queue
	// Shouldn't need a go routine here because it should be
	// blocking so each worker will only have a single worker
	handleOutgoing(deliveries, c.Done, relay)
}

// Handle Outgoing messages from the queue
func handleOutgoing(deliveries <-chan amqp.Delivery, done chan error, relay *Relay) {
	for d := range deliveries {
		status := transfer(d.Body, relay)
		if status {
			d.Ack(true)
		}
	}

	done <- nil
}

func transfer(msg []byte, relay *Relay) bool {
	node := relay.Addr

	tcpAddr, err := net.ResolveTCPAddr("tcp4", node)
	if err != nil {
		fmt.Printf("Fatal error: %s", err.Error())
		return false
	}

	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		fmt.Printf("Fatal error: %s", err.Error())
		return false
	}
	defer conn.Close()

	_, err = conn.Write(msg)
	if err != nil {
		fmt.Printf("Fatal error: %s", err.Error())
		return false
	}

	return true
}

func checkError(err error) {
	if err != nil {
		fmt.Printf("Fatal error: %s", err.Error())
	}
}
