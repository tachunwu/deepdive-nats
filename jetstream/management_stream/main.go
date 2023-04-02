package main

import (
	"encoding/json"
	"fmt"

	natsclient "github.com/nats-io/nats.go"
)

func main() {
	nc, _ := natsclient.Connect(natsclient.DefaultURL)
	js, _ := nc.JetStream()

	name := "Default"
	CreateStream(js, name)
	PrintStreamState(js, "Default")

}

func CreateStream(js natsclient.JetStreamContext, name string) {
	js.AddStream(&natsclient.StreamConfig{
		Name:     name,
		Subjects: []string{"events.>"},
	})
}

func PrintStreamState(js natsclient.JetStreamContext, name string) {
	info, _ := js.StreamInfo(name)
	b, _ := json.MarshalIndent(info, "", " ")
	fmt.Println("inspecting stream info")
	fmt.Println(string(b))
}
