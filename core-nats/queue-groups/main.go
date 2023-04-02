package main

import (
	"log"
	"runtime"

	natsclient "github.com/nats-io/nats.go"
	"github.com/spf13/cast"
)

func main() {
	nc, _ := natsclient.Connect(natsclient.DefaultURL)
	QueueSubscribe(nc, 0)
	QueueSubscribe(nc, 1)
	QueueSubscribe(nc, 2)
	PublishTask(nc)

	runtime.Goexit()
}

func QueueSubscribe(nc *natsclient.Conn, workerId int) {
	nc.QueueSubscribe("tasks", "pool", func(m *natsclient.Msg) {
		log.Println("worker", workerId, "process task", string(m.Data))
	})
}

func PublishTask(nc *natsclient.Conn) {
	for i := 0; i < 10; i++ {
		nc.Publish("tasks", []byte(cast.ToString(i)))
	}
}
