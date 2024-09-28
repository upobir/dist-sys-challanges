package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type SendBody struct {
	Key string `json:"key"`
	Msg int    `json:"msg"`
}

type OffsetsBody struct {
	Offsets map[string]int `json:"offsets"`
}

type KeyListBody struct {
	Keys []string `json:"keys"`
}

var logsMu sync.RWMutex
var logs map[string][]int
var commitsMu sync.RWMutex
var commits map[string]int
var n *maelstrom.Node

func main() {
	n = maelstrom.NewNode()
	logs = map[string][]int{}
	commits = map[string]int{}

	n.Handle("send", sendHandler)
	n.Handle("poll", pollHandler)
	n.Handle("commit_offsets", commitOffsetsHandler)
	n.Handle("list_committed_offsets", listCommittedOffsetsHandler)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func sendHandler(msg maelstrom.Message) error {
	var body SendBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return nil
	}

	logsMu.Lock()
	logsForKey, contains := logs[body.Key]
	if !contains {
		logsForKey = []int{}
	}
	offset := len(logsForKey)
	logs[body.Key] = append(logsForKey, body.Msg)
	logsMu.Unlock()

	return n.Reply(msg, map[string]any{"type": "send_ok", "offset": offset})
}

func pollHandler(msg maelstrom.Message) error {
	var body OffsetsBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return nil
	}

	msgs := map[string][][2]int{}

	logsMu.RLock()
	for key, offset := range body.Offsets {
		logsToSend := [][2]int{}
		logsForKey, contains := logs[key]
		if contains {
			for index := offset; index < len(logsForKey); index++ {
				logsToSend = append(logsToSend, [2]int{index, logsForKey[index]})
			}
		}
		msgs[key] = logsToSend
	}
	logsMu.RUnlock()

	return n.Reply(msg, map[string]any{"type": "poll_ok", "msgs": msgs})
}

func commitOffsetsHandler(msg maelstrom.Message) error {
	var body OffsetsBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return nil
	}

	commitsMu.Lock()
	for key, offset := range body.Offsets {
		oldOffset := commits[key]
		if offset < oldOffset {
			offset = oldOffset
		}
		commits[key] = offset
	}
	commitsMu.Unlock()

	return n.Reply(msg, map[string]any{"type": "commit_offsets_ok"})
}

func listCommittedOffsetsHandler(msg maelstrom.Message) error {
	var body KeyListBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return nil
	}

	offsets := map[string]int{}
	commitsMu.RLock()
	for _, key := range body.Keys {
		offset, contains := commits[key]
		if contains {
			offsets[key] = offset
		}
	}
	commitsMu.RUnlock()
	return n.Reply(msg, map[string]any{"type": "list_committed_offsets_ok", "offsets": offsets})
}
