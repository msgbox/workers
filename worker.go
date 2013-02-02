package workers

import (
	"fmt"
	"github.com/msgbox/queue"
	"github.com/msgbox/workers/workers"
	"github.com/streadway/amqp"
)

type workers struct {
	queue *amqp.Connection
}

var workerConn *workers

func init() {
	conn, err := queue.Connect()
	if err != nil {
		fmt.Printf("Fatal error: %s", err.Error())
	}

	workerConn = &workers{queue: conn}
}

func CreateIncoming(tag string) {
	worker.Incoming(tag, workerConn.queue)
}

func CreateOutgoing(tag string, relay string) {
	r := worker.Relay{Addr: relay}
	worker.Outgoing(tag, workerConn.queue, &r)
}
