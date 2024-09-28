package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

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

var n *maelstrom.Node
var kv *maelstrom.KV

func main() {
	n = maelstrom.NewNode()
	kv = maelstrom.NewLinKV(n)

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

	var offset int
	for {
		var oldLogs []int
		if err := kv.ReadInto(context.Background(), "logs/"+body.Key, &oldLogs); err != nil {
			oldLogs = []int{}
		}
		offset = len(oldLogs)
		newLogs := append(oldLogs, body.Msg)

		if err := kv.CompareAndSwap(context.Background(), "logs/"+body.Key, oldLogs, newLogs, true); err == nil {
			break
		}

		time.Sleep(200 * time.Millisecond)
	}

	return n.Reply(msg, map[string]any{"type": "send_ok", "offset": offset})
}

func pollHandler(msg maelstrom.Message) error {
	var body OffsetsBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return nil
	}

	msgs := map[string][][2]int{}
	for key, offset := range body.Offsets {
		logsToSend := [][2]int{}

		var logs []int
		if err := kv.ReadInto(context.Background(), "logs/"+key, &logs); err != nil {
			logs = []int{}
		}

		for index := offset; index < len(logs); index++ {
			logsToSend = append(logsToSend, [2]int{index, logs[index]})
		}
		msgs[key] = logsToSend
	}

	return n.Reply(msg, map[string]any{"type": "poll_ok", "msgs": msgs})
}

func commitOffsetsHandler(msg maelstrom.Message) error {
	var body OffsetsBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return nil
	}

	updates := body.Offsets
	for {
		var oldOffsets map[string]int
		if err := kv.ReadInto(context.Background(), "commits", &oldOffsets); err != nil {
			oldOffsets = map[string]int{}
		}

		newOffsets := map[string]int{}
		for key, offset := range oldOffsets {
			newOffsets[key] = offset
		}

		for key, newOffset := range updates {
			oldOffset := newOffsets[key]
			if newOffset >= oldOffset {
				newOffsets[key] = newOffset
			}
		}

		if err := kv.CompareAndSwap(context.Background(), "commits", oldOffsets, newOffsets, true); err == nil {
			break
		}

		time.Sleep(200 * time.Millisecond)
	}

	return n.Reply(msg, map[string]any{"type": "commit_offsets_ok"})
}

func listCommittedOffsetsHandler(msg maelstrom.Message) error {
	var body KeyListBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return nil
	}

	var commits map[string]int
	if err := kv.ReadInto(context.Background(), "commits", &commits); err != nil {
		commits = map[string]int{}
	}

	offsets := map[string]int{}
	for _, key := range body.Keys {
		offset, contains := commits[key]
		if contains {
			offsets[key] = offset
		}
	}
	return n.Reply(msg, map[string]any{"type": "list_committed_offsets_ok", "offsets": offsets})
}
