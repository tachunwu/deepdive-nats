package main

import (
	"log"
	"runtime"
	"time"

	natsclient "github.com/nats-io/nats.go"
)

func main() {
	nc, _ := natsclient.Connect(natsclient.DefaultURL)
	go Pub(nc)
	go AsyncSub(nc)
	go SyncSub(nc)
	runtime.Goexit()
}

func Pub(nc *natsclient.Conn) {
	for {
		nc.Publish("msg.app.test", []byte("Pub subject: msg.app.test"))
		time.Sleep(time.Second)
		nc.Publish("msg.app", []byte("Pub subject: msg.app"))
		time.Sleep(time.Second)
	}

}

func AsyncSub(nc *natsclient.Conn) {
	nc.Subscribe("msg.*", func(m *natsclient.Msg) {
		log.Println("AsyncSub msg.*", string(m.Data))
	})
}

func SyncSub(nc *natsclient.Conn) {
	sub, err := nc.SubscribeSync("msg.>")
	if err != nil {
		return
	}
	for {
		m, err := sub.NextMsg(10 * time.Second)
		if err != nil {
			return
		}
		log.Println("SyncSub msg.>", string(m.Data))
	}

}
