package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type KafkaNode struct {
	n                 *maelstrom.Node
	logs              map[string][]float64
	committedMessages map[string]float64
	logLock           sync.RWMutex
}

func (kn *KafkaNode) sendHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	kn.logLock.Lock()
	key := body["key"].(string)
	newMsg := body["msg"].(float64)
	kn.logs[key] = append(kn.logs[key], newMsg)
	offset := len(kn.logs[key]) - 1
	kn.logLock.Unlock()

	res := make(map[string]any)
	res["type"] = "send_ok"
	res["offset"] = offset
	return kn.n.Reply(msg, res)
}

func (kn *KafkaNode) pollHandler(msg maelstrom.Message) error {
	type pollMsg struct {
		Offsets map[string]float64 `json:"offsets"`
	}
	var body pollMsg
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	kn.logLock.RLock()
	msgs := make(map[string][][]float64)
	for key, offset := range body.Offsets {
		if offset >= float64(len(kn.logs[key])) {
			continue // no messages exist at this offset
		}
		res := make([][]float64, 1)
		res[0] = []float64{offset, kn.logs[key][int(offset)]}
		msgs[key] = res
	}
	kn.logLock.RUnlock()

	res := make(map[string]any)
	res["type"] = "poll_ok"
	res["msgs"] = msgs
	return kn.n.Reply(msg, res)
}

func (kn *KafkaNode) committedOffsetsHandler(msg maelstrom.Message) error {
	type committedMsg struct {
		Offsets map[string]float64 `json:"offsets"`
	}
	var body committedMsg
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	kn.logLock.Lock()
	for key, offset := range body.Offsets {
		kn.committedMessages[key] = offset
	}
	kn.logLock.Unlock()

	res := make(map[string]any)
	res["type"] = "commit_offsets_ok"
	return kn.n.Reply(msg, res)
}

func (kn *KafkaNode) listCommittedOffsetsHandler(msg maelstrom.Message) error {
	type listCommittedMsg struct {
		Keys []string `json:"keys"`
	}
	var body listCommittedMsg
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	kn.logLock.RLock()
	offsets := make(map[string]float64)
	for _, key := range body.Keys {
		offsets[key] = kn.committedMessages[key]
	}
	kn.logLock.RUnlock()

	res := make(map[string]any)
	res["type"] = "list_committed_offsets_ok"
	res["offsets"] = offsets
	return kn.n.Reply(msg, res)
}

func main() {
	kn := KafkaNode{
		n:                 maelstrom.NewNode(),
		logs:              make(map[string][]float64),
		committedMessages: make(map[string]float64),
	}

	kn.n.Handle("send", kn.sendHandler)
	kn.n.Handle("poll", kn.pollHandler)
	kn.n.Handle("commit_offsets", kn.committedOffsetsHandler)
	kn.n.Handle("list_committed_offsets", kn.listCommittedOffsetsHandler)

	if err := kn.n.Run(); err != nil {
		log.Fatal(err)
	}
}
