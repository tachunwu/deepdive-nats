package main

import (
	"log"
	"runtime"
	"time"

	natsclient "github.com/nats-io/nats.go"
)

func main() {
	nc, _ := natsclient.Connect(natsclient.DefaultURL)

	go JetstreamPub(nc)
	go QueuePub(nc)
	runtime.Goexit()
}

func JetstreamPub(nc *natsclient.Conn) {
	js, _ := nc.JetStream()
	for {
		log.Println("Publishing event to Jetstream: event.push")
		js.Publish("events.push", []byte("Pub subject to Jetstream: events.push"))
		time.Sleep(time.Second * 5)
		log.Println("Publishing event to Jetstream: event.pull")
		js.Publish("events.pull", []byte("Pub subject to Jetstream: events.pull"))
		time.Sleep(time.Second * 5)
	}
}

func QueuePub(nc *natsclient.Conn) {
	js, _ := nc.JetStream()
	for {
		log.Println("Publishing event to Jetstream: event.queue")
		js.Publish("events.queue", []byte("Pub subject to Jetstream: events.queue"))
		time.Sleep(time.Millisecond * 100)
	}
}
