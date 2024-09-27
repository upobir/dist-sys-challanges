package main

import (
	"encoding/json"
	"log"
	"sort"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Topology = map[string]([]string)

type TopologyBody struct {
	Topology Topology `json:"topology"`
}

type BroadcastBody struct {
	Value int `json:"message"`
}

type NodeBroadcastBody struct {
	Values []int `json:"message"`
}

var seen map[int]bool
var seenMu sync.RWMutex
var unacked map[string](map[int]time.Time)
var unackedMu sync.RWMutex
var topology Topology
var n *maelstrom.Node

func main() {
	n = maelstrom.NewNode()
	seen = map[int]bool{}
	unacked = map[string](map[int]time.Time){}

	n.Handle("broadcast", broadcastHandler)
	n.Handle("node-broadcast", nodeBroadcastHandler)
	n.Handle("read", readHandler)
	n.Handle("topology", topologyHandler)

	go announce()

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func announce() {
	for {
		time.Sleep(100 * time.Millisecond)

		unackedMu.Lock()
		for node, values := range unacked {
			announceList := []int{}
			now := time.Now()
			for value, sentAt := range values {
				diff := now.Sub(sentAt)
				if diff.Milliseconds() > 250 {
					announceList = append(announceList, value)
				}
			}

			for _, value := range announceList {
				values[value] = now
			}

			if len(announceList) > 0 {
				gossip(node, announceList)
			}
		}
		unackedMu.Unlock()
	}
}

func addValueToSeen(value int) bool {
	var result bool

	_, contains := seen[value]
	if contains {
		result = false
	} else {
		seen[value] = true
		result = true
	}

	return result
}

func gossip(node string, values []int) {
	body := map[string]any{
		"type":    "node-broadcast",
		"message": values,
	}
	n.RPC(node, body, func(msg maelstrom.Message) error {
		unackedMu.Lock()
		for _, value := range values {
			delete(unacked[node], value)
		}
		unackedMu.Unlock()
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

	seenMu.Lock()
	newValue := addValueToSeen(body.Value)
	seenMu.Unlock()

	unackedMu.Lock()
	if newValue {
		for _, node := range topology[n.ID()] {
			if node == msg.Src {
				continue
			}
			unacked[node][body.Value] = time.Time{}
		}
	}
	unackedMu.Unlock()
	return nil
}

func nodeBroadcastHandler(msg maelstrom.Message) error {
	var body NodeBroadcastBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	result := map[string]string{"type": "ack"}
	if err := n.Reply(msg, result); err != nil {
		return err
	}

	seenMu.Lock()
	unackedMu.Lock()
	for _, value := range body.Values {
		newValue := addValueToSeen(value)

		if newValue {
			for _, node := range topology[n.ID()] {
				if node == msg.Src {
					continue
				}
				unacked[node][value] = time.Time{}
			}
		}
	}
	unackedMu.Unlock()
	seenMu.Unlock()

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

	unackedMu.Lock()
	for _, node := range topology[n.ID()] {
		unacked[node] = map[int]time.Time{}
	}
	unackedMu.Unlock()

	return nil
}
