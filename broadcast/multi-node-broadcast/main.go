package main

import (
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"sync"
)

type Topology = map[string]([]string)

type TopologyBody struct {
	Topology Topology `json:"topology"`
}

type BroadcastBody struct {
	Value int `json:"message"`
}

var seen map[int]bool
var mu sync.RWMutex
var topology Topology
var n *maelstrom.Node

func main() {
	n = maelstrom.NewNode()
	seen = map[int]bool{}

	n.Handle("broadcast", broadcastHandler)
	n.Handle("node-broadcast", nodeBroadcastHandler)
	n.Handle("read", readHandler)
	n.Handle("topology", topologyHandler)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func addValueToSeen(value int, self string) {
	mu.Lock()
	defer mu.Unlock()

	if _, contains := seen[value]; !contains {
		seen[value] = true
		for _, node := range topology[self] {
			n.Send(node, map[string]any{
				"type":    "node-broadcast",
				"message": value,
			})
		}
	}
}

func broadcastHandler(msg maelstrom.Message) error {
	var body BroadcastBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	result := map[string]string{"type": "broadcast_ok"}
	if err := n.Reply(msg, result); err != nil {
		return err
	}

	addValueToSeen(body.Value, msg.Dest)

	return nil
}

func nodeBroadcastHandler(msg maelstrom.Message) error {
	var body BroadcastBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	addValueToSeen(body.Value, msg.Dest)
	return nil
}

func readHandler(msg maelstrom.Message) error {
	result := map[string]any{"type": "read_ok"}
	seenList := []int{}

	mu.RLock()
	defer mu.RUnlock()

	for k := range seen {
		seenList = append(seenList, k)
	}

	result["messages"] = seenList
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
