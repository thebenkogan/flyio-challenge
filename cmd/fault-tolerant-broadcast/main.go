package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const GOSSIP_RPC = "gossip"
const BACKOFF_BASE = 0.5 // back off start time in seconds

type BroadcastNode struct {
	n         *maelstrom.Node
	messages  map[float64]bool
	neighbors []string
	msgLock   sync.RWMutex
}

func (bn *BroadcastNode) getMsgArray() []float64 {
	bn.msgLock.RLock()
	defer bn.msgLock.RUnlock()
	msgArr := make([]float64, len(bn.messages))
	i := 0
	for k := range bn.messages {
		msgArr[i] = k
		i++
	}
	return msgArr
}

func (bn *BroadcastNode) addMsg(val float64) {
	bn.msgLock.Lock()
	defer bn.msgLock.Unlock()
	bn.messages[val] = true
}

func (bn *BroadcastNode) hasMsg(val float64) bool {
	bn.msgLock.RLock()
	defer bn.msgLock.RUnlock()
	_, ok := bn.messages[val]
	return ok
}

func (bn *BroadcastNode) sendMessageWithBackoff(dst string, body map[string]any) {
	msgReceived := false
	backoff := BACKOFF_BASE
	send := func() {
		bn.n.RPC(dst, body, func(msg maelstrom.Message) error {
			var body map[string]any
			if err := json.Unmarshal(msg.Body, &body); err != nil {
				return err
			}
			msgReceived = body["type"].(string) == "gossip_ok"
			return nil
		})
	}
	for !msgReceived {
		send()
		time.Sleep(time.Duration(backoff * float64(time.Second)))
		backoff *= 2
	}
}

func (bn *BroadcastNode) gossipMsg(val float64) error {
	gossip := make(map[string]any)
	gossip["type"] = GOSSIP_RPC
	gossip["value"] = val
	for _, neighbor := range bn.neighbors {
		go bn.sendMessageWithBackoff(neighbor, gossip)
	}
	return nil
}

func (bn *BroadcastNode) broadcastHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	val := body["message"].(float64)
	bn.addMsg(val)
	if err := bn.gossipMsg(val); err != nil {
		return err
	}

	res := make(map[string]any)
	res["type"] = "broadcast_ok"
	return bn.n.Reply(msg, res)
}

func (bn *BroadcastNode) readHandler(msg maelstrom.Message) error {
	res := make(map[string]any)
	res["type"] = "read_ok"
	res["messages"] = bn.getMsgArray()
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

func (bn *BroadcastNode) gossipHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	val := body["value"].(float64)
	if !bn.hasMsg(val) {
		bn.addMsg(val)
		if err := bn.gossipMsg(val); err != nil {
			return err
		}
	}

	res := make(map[string]any)
	res["type"] = "gossip_ok"
	return bn.n.Reply(msg, res)
}

func main() {
	bn := BroadcastNode{
		n:         maelstrom.NewNode(),
		messages:  make(map[float64]bool),
		neighbors: make([]string, 5),
	}

	bn.n.Handle("broadcast", bn.broadcastHandler)
	bn.n.Handle("read", bn.readHandler)
	bn.n.Handle("topology", bn.topologyHandler)
	bn.n.Handle(GOSSIP_RPC, bn.gossipHandler)

	if err := bn.n.Run(); err != nil {
		log.Fatal(err)
	}
}
