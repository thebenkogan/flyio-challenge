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
		delta := int(body["delta"].(float64))

		addMutex.Lock()
		success := false
		for !success {
			success = true
			prev, err := kv.ReadInt(ctx, "counter")
			if err != nil {
				prev = 0
			}
			if err := kv.CompareAndSwap(ctx, "counter", prev, prev+delta, true); err != nil {
				success = false
			}
		}
		addMutex.Unlock()

		res := make(map[string]any)
		res["type"] = "add_ok"
		return n.Reply(msg, res)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		ctx := context.Background()

		total, err := kv.ReadInt(ctx, "counter")
		if err != nil {
			return err
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
