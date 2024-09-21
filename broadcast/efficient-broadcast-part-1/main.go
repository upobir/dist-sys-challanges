package main

import (
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"sort"
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

func addValueToSeen(value int) bool {
	var result bool

	seenMu.Lock()
	_, contains := seen[value]
	if contains {
		result = false
	} else {
		seen[value] = true
		result = true
	}
	seenMu.Unlock()

	return result
}

func gossip(value int, node string, unacked map[string]bool, unackMu *sync.RWMutex) {
	body := map[string]any{
		"type":    "broadcast",
		"message": value,
	}
	n.RPC(node, body, func(msg maelstrom.Message) error {
		unackMu.Lock()
		delete(unacked, node)
		unackMu.Unlock()
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

	newValue := addValueToSeen(body.Value)

	unacked := map[string]bool{}
	var unackMu sync.RWMutex

	if newValue {
		for _, node := range topology[n.ID()] {
			if node == msg.Src {
				continue
			}
			unacked[node] = true
		}
	}

	for {
		unackMu.RLock()
		if len(unacked) == 0 {
			unackMu.RUnlock()
			break
		}
		gossipList := []string{}
		for node := range unacked {
			gossipList = append(gossipList, node)
		}
		unackMu.RUnlock()

		for _, node := range gossipList {
			gossip(body.Value, node, unacked, &unackMu)
		}

		time.Sleep(1000 * time.Millisecond)
	}

	return nil
}

func readHandler(msg maelstrom.Message) error {
	result := map[string]any{"type": "read_ok"}
	seenList := []int{}

	seenMu.RLock()
	for k := range seen {
		seenList = append(seenList, k)
	}
	seenMu.RUnlock()

	result["messages"] = seenList
	return n.Reply(msg, result)
}

func topologyHandler(msg maelstrom.Message) error {
	var body TopologyBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	result := map[string]string{"type": "topology_ok"}
	if err := n.Reply(msg, result); err != nil {
		return err
	}

	// building a tree with 4 spread ratio
	topology = map[string][]string{}

	nodes := n.NodeIDs()
	sort.Strings(nodes)
	degree := 4

	for i := 0; i < len(nodes); i++ {
		topology[nodes[i]] = []string{}
	}

	for i := 0; i < len(nodes); i++ {
		for j := degree*i + 1; j <= degree*i+degree && j < len(nodes); j++ {
			topology[nodes[j]] = append(topology[nodes[j]], nodes[i])
			topology[nodes[i]] = append(topology[nodes[i]], nodes[j])
		}
	}

	return nil
}
