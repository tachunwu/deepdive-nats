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

	CreatePullConsumer(nc, "Default", "PullConsumer")
	PullConsumerSubscribe(nc, 1)
	runtime.Goexit()
}

func CreatePullConsumer(nc *natsclient.Conn, stream string, durable string) {
	js, _ := nc.JetStream()

	info, err := js.AddConsumer(stream, &natsclient.ConsumerConfig{
		Durable:       durable,
		FilterSubject: "events.pull",
		AckPolicy:     natsclient.AckExplicitPolicy,
	})

	if err != nil {
		fmt.Println(err)
	}

	b, _ := json.MarshalIndent(info, "", " ")
	fmt.Println("inspecting consumer info")
	fmt.Println(string(b))
}

func PullConsumerSubscribe(nc *natsclient.Conn, batch int) {
	js, _ := nc.JetStream()
	sub, err := js.PullSubscribe("events.pull", "PullConsumer", natsclient.PullMaxWaiting(512))
	if err != nil {
		fmt.Println(err)
		return
	}
	for {
		msgs, err := sub.Fetch(batch)
		if err != nil {
			fmt.Println(err)
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
