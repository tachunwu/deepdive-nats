package main

import (
	"log"
	"runtime"
	"time"

	natsclient "github.com/nats-io/nats.go"
)

func main() {
	nc, _ := natsclient.Connect(natsclient.DefaultURL)
	go Reply(nc)
	go Request(nc)

	runtime.Goexit()
}

func Reply(nc *natsclient.Conn) {
	nc.Subscribe("time", func(m *natsclient.Msg) {
		m.Respond([]byte(time.Now().String()))
	})
}

func Request(nc *natsclient.Conn) {
	for {
		m, err := nc.Request("time", nil, 10*time.Second)
		if err != nil {
			return
		}
		log.Println(string(m.Data))
		time.Sleep(time.Second)
	}
}
