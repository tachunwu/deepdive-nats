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

	CreatePushConsumer(nc, "Default", "PushConsumer")
	PushConsumerSubscribe(nc)
	runtime.Goexit()
}

func CreatePushConsumer(nc *natsclient.Conn, stream string, durable string) {
	js, _ := nc.JetStream()

	info, err := js.AddConsumer(stream, &natsclient.ConsumerConfig{
		Durable:        durable,
		DeliverSubject: "PushSubject",
		FilterSubject:  "events.push",
		AckPolicy:      natsclient.AckExplicitPolicy,
	})

	if err != nil {
		fmt.Println(err)
	}

	b, _ := json.MarshalIndent(info, "", " ")
	fmt.Println("inspecting consumer info")
	fmt.Println(string(b))
}

func PushConsumerSubscribe(nc *natsclient.Conn) {
	js, _ := nc.JetStream()
	sub, err := js.SubscribeSync(
		"events.push",
		natsclient.Durable("PushConsumer"),
	)
	if err != nil {
		fmt.Println(err)
		return
	}
	for {
		m, err := sub.NextMsg(time.Second)
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		meta, _ := m.Metadata()
		b, _ := json.MarshalIndent(meta, "", " ")
		fmt.Println(string(b), string(m.Data))
		m.Ack()

	}

}
