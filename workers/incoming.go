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
	checkError(err)

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
	checkError(err)

	// Handle messages coming off the queue
	// Shouldn't need a go routine here because it should be
	// blocking so each worker will only have a single worker
	handleIncoming(deliveries, c.Done)
}

// Handle Incoming Items off the queue
func handleIncoming(deliveries <-chan amqp.Delivery, done chan error) {
	for d := range deliveries {
		status := saveMessage(d.Body)
		if status {
			d.Ack(true)
		}
	}

	done <- nil
}

func saveMessage(data []byte) bool {
	msg := messages.Parse(data)

	// Get Name and Box
	m_name, m_box := messages.ParseReceiver(msg.GetReceiver())

	// Lookup Name and Box in data store to ensure they exist
	account := storage.FindAccount(m_name)
	if account == nil {
		fmt.Println("Account does not exist")
		return false
	}

	box := storage.FindBox(*&account.Id, m_box)
	if box == nil {
		fmt.Println("Account does not have a box with that name")
		return false
	}

	// Insert Message into Database
	err := storage.InsertMessage(account.Id, box.Id, *&msg)
	if err != nil {
		fmt.Printf("Error saving Message: %s", err)
		return false
	}

	return true
}
