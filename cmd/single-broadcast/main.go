package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type BroadcastNode struct {
	n         *maelstrom.Node
	messages  []float64
	neighbors []string
	msgLock   sync.RWMutex
}

func (bn *BroadcastNode) addMsg(val float64) {
	bn.msgLock.Lock()
	defer bn.msgLock.Unlock()
	bn.messages = append(bn.messages, val)
}

func (bn *BroadcastNode) broadcastHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	val := body["message"].(float64)
	bn.addMsg(val)

	res := make(map[string]any)
	res["type"] = "broadcast_ok"
	return bn.n.Reply(msg, res)
}

func (bn *BroadcastNode) readHandler(msg maelstrom.Message) error {
	bn.msgLock.RLock()
	defer bn.msgLock.RUnlock()
	res := make(map[string]any)
	res["type"] = "read_ok"
	res["messages"] = bn.messages
	return bn.n.Reply(msg, res)
}

func (bn *BroadcastNode) topologyHandler(msg maelstrom.Message) error {
	type topologyMsg struct {
		Topology map[string][]string `json:"topology"`
	}
	var body topologyMsg
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	bn.neighbors = body.Topology[bn.n.ID()]

	res := make(map[string]any)
	res["type"] = "topology_ok"
	return bn.n.Reply(msg, res)
}

func main() {
	bn := BroadcastNode{
		n:         maelstrom.NewNode(),
		messages:  make([]float64, 10),
		neighbors: make([]string, 0),
	}

	bn.n.Handle("broadcast", bn.broadcastHandler)
	bn.n.Handle("read", bn.readHandler)
	bn.n.Handle("topology", bn.topologyHandler)

	if err := bn.n.Run(); err != nil {
		log.Fatal(err)
	}
}
