package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type AddBody struct {
	Delta int `json:"delta"`
}

type StoredData struct {
	Value     int
	Timestamp int64
}

var n *maelstrom.Node
var kv *maelstrom.KV

func main() {
	n = maelstrom.NewNode()
	kv = maelstrom.NewSeqKV(n)

	n.Handle("add", addHandler)
	n.Handle("read", readHandler)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func update(delta int) int {
	for {
		var oldData StoredData
		ts := time.Now().UnixMilli()

		err := kv.ReadInto(context.Background(), "value", &oldData)
		if err != nil {
			oldData = StoredData{
				Value:     0,
				Timestamp: ts,
			}
		}

		newData := StoredData{
			Value:     oldData.Value + delta,
			Timestamp: ts,
		}

		err = kv.CompareAndSwap(context.Background(), "value", oldData, newData, true)
		if err == nil {
			return newData.Value
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func addHandler(msg maelstrom.Message) error {
	var body AddBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	update(body.Delta)

	result := map[string]string{"type": "add_ok"}
	return n.Reply(msg, result)
}

func readHandler(msg maelstrom.Message) error {
	value := update(0)

	result := map[string]any{"type": "read_ok", "value": value}
	return n.Reply(msg, result)
}
