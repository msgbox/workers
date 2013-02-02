package worker

import (
	"fmt"
	"github.com/msgbox/message"
	"github.com/msgbox/queue"
	"github.com/msgbox/storage"
	"github.com/streadway/amqp"
)

// An Incoming Worker should listen for new items on the incoming
// queue and process them as necessary

func Incoming(tag string, connection *amqp.Connection) {

	// Create a Consumer
	c, err := queue.BuildConsumer(tag, connection)
	if err != nil {
		fmt.Errorf("Consumer Error: %s", err)
	}

	// Start the Worker
	deliveries, err := c.Channel.Consume(
		"incoming_messages", // name
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

	go handleIncoming(deliveries, c.Done)

}

// Handle Incoming Items off the queue
func handleIncoming(deliveries <-chan amqp.Delivery, done chan error) {
	for d := range deliveries {
		msg := messages.Parse(d.Body)

		// Get Name and Box
		m_name, m_box := messages.ParseReceiver(msg.GetReceiver())

		// Lookup Name and Box in data store to ensure they exist
		account := storage.FindAccount(m_name)
		if account == nil {
			// Handle non-existing account
			fmt.Println("Account does not exist")
		}

		box := storage.FindBox(*&account.Id, m_box)
		if box == nil {
			// Handle non-existing account
			fmt.Println("Account does not have a box with that name")
		}

		// Insert Message into Database
		err := storage.InsertMessage(account.Id, box.Id, *&msg)
		if err != nil {
			fmt.Printf("Error saving Message: %s", err)
		}

		d.Ack(true)
	}

	done <- nil
}
