package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)
	addMutex := sync.Mutex{}

	n.Handle("add", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		ctx := context.Background()

		addMutex.Lock()
		delta := int(body["delta"].(float64))
		prev, err := kv.ReadInt(ctx, n.ID())
		if err != nil {
			prev = 0
		}
		if err := kv.Write(ctx, n.ID(), prev+delta); err != nil {
			return err
		}
		addMutex.Unlock()

		res := make(map[string]any)
		res["type"] = "add_ok"
		return n.Reply(msg, res)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		ctx := context.Background()

		total := 0
		for _, node := range n.NodeIDs() {
			val, err := kv.ReadInt(ctx, node)
			if err != nil {
				val = 0
			}
			total += val
		}

		res := make(map[string]any)
		res["type"] = "read_ok"
		res["value"] = total
		return n.Reply(msg, res)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
