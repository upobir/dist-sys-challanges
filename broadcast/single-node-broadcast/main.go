package main

import (
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
)

type Topology = map[string]([]string)

type TopologyBody struct {
	Topology Topology `json:"topology"`
}

type BroadcastBody struct {
	Value int `json:"message"`
}

var seen []int
var topology Topology
var n *maelstrom.Node

func main() {
	n = maelstrom.NewNode()
	seen = []int{}

	n.Handle("broadcast", broadcastHandler)
	n.Handle("read", readHandler)
	n.Handle("topology", topologyHandler)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func broadcastHandler(msg maelstrom.Message) error {
	var body BroadcastBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	seen = append(seen, body.Value)

	result := map[string]string{"type": "broadcast_ok"}
	return n.Reply(msg, result)
}

func readHandler(msg maelstrom.Message) error {
	result := map[string]any{"type": "read_ok", "messages": seen}
	return n.Reply(msg, result)
}

func topologyHandler(msg maelstrom.Message) error {
	var body TopologyBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	topology = body.Topology
	_ = topology

	result := map[string]string{"type": "topology_ok"}
	return n.Reply(msg, result)
}
