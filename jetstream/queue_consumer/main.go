package main

import (
	"encoding/json"
	"fmt"
	"runtime"
	"time"

	natsclient "github.com/nats-io/nats.go"
)

func main() {
	nc, _ := natsclient.Connect(natsclient.DefaultURL)

	CreateQueueGroupConsumer(nc, "Default", "QueueGroupConsumer")
	QueueGroupConsumerSubscribe(nc)
	runtime.Goexit()
}

func CreateQueueGroupConsumer(nc *natsclient.Conn, stream string, durable string) {
	js, _ := nc.JetStream()

	info, err := js.ConsumerInfo("Default", "QueueGroupConsumer")
	if err != nil {
		fmt.Println(err)
		return
	}

	if info != nil {
		fmt.Println("Consumer already exists")
		return
	}
	info, err = js.AddConsumer(stream, &natsclient.ConsumerConfig{
		Durable:        "QueueGroupConsumer",
		DeliverSubject: "QueueGroupSubject",
		DeliverGroup:   "queue-group",
		AckPolicy:      natsclient.AckExplicitPolicy,
		FilterSubject:  "events.queue",
	})
	if err != nil {
		fmt.Println(err)
	}

	b, _ := json.MarshalIndent(info, "", " ")
	fmt.Println("inspecting consumer info")
	fmt.Println(string(b))
}

func QueueGroupConsumerSubscribe(nc *natsclient.Conn) {
	js, _ := nc.JetStream()

	sub, err := js.PullSubscribe(
		"events.queue",
		"queue-group",
		natsclient.MaxAckPending(1))

	if err != nil {
		fmt.Println(err)
		return
	}

	for {
		msgs, err := sub.Fetch(1, natsclient.MaxWait(time.Second))
		if err != nil {
			time.Sleep(time.Second)
			continue
		}

		for _, m := range msgs {
			meta, _ := m.Metadata()
			b, _ := json.MarshalIndent(meta, "", " ")
			fmt.Println(string(b), string(m.Data))
			m.Ack()
		}
	}
}
