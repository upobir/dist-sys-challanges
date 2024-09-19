package main

import (
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"strconv"
)

func main() {
	n := maelstrom.NewNode()

	cur_id := 0

	n.Handle("generate", func(msg maelstrom.Message) error {
		body := map[string]any{}
		body["type"] = "generate_ok"
		body["id"] = msg.Dest + "-" + strconv.Itoa(cur_id)
		cur_id++
		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
