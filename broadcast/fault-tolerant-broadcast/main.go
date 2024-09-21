package main

import (
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"sync"
	"time"
)

type Topology = map[string]([]string)

type TopologyBody struct {
	Topology Topology `json:"topology"`
}

type BroadcastBody struct {
	Value int `json:"message"`
}

var seen map[int]bool
var seenMu sync.RWMutex
var topology Topology
var n *maelstrom.Node

func main() {
	n = maelstrom.NewNode()
	seen = map[int]bool{}

	n.Handle("broadcast", broadcastHandler)
	n.Handle("read", readHandler)
	n.Handle("topology", topologyHandler)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func addValueToSeen(value int, self string, unacked map[string]bool, unackMu *sync.RWMutex) {
	seenMu.Lock()
	defer seenMu.Unlock()

	if _, contains := seen[value]; !contains {
		seen[value] = true
		for _, node := range topology[self] {
			unacked[node] = true
			gossip(value, node, unacked, unackMu)
		}
	}
}

func gossip(value int, node string, unacked map[string]bool, unackMu *sync.RWMutex) {
	body := map[string]any{
		"type":    "broadcast",
		"message": value,
	}
	n.RPC(node, body, func(msg maelstrom.Message) error {
		unackMu.Lock()
		defer unackMu.Unlock()
		delete(unacked, node)
		return nil
	})
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

	unacked := map[string]bool{}
	var unackMu sync.RWMutex
	addValueToSeen(body.Value, msg.Dest, unacked, &unackMu)

	for {
		unackMu.RLock()
		if len(unacked) == 0 {
			break
		}

		for node := range unacked {
			gossip(body.Value, node, unacked, &unackMu)
		}
		unackMu.RUnlock()

		time.Sleep(100 * time.Millisecond)
	}

	return nil
}

func readHandler(msg maelstrom.Message) error {
	result := map[string]any{"type": "read_ok"}
	seenList := []int{}

	seenMu.RLock()
	defer seenMu.RUnlock()

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
