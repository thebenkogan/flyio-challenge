package main

import (
	"context"
	"encoding/json"
	"log"
	"strconv"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// idea: have individual kv entries for each message along with a latest offset entry
// - on insertion, increment the latest offset entry via CAS to claim next offset entry

func offsetKey(key string, offset int) string {
	return key + "-" + strconv.Itoa(offset)
}

func latestOffsetKey(key string) string {
	return "latest-" + key
}

func committedOffsetKey(key string) string {
	return "committed-" + key
}

type KafkaNode struct {
	n  *maelstrom.Node
	kv *maelstrom.KV
}

func (kn *KafkaNode) claimNextOffset(ctx context.Context, key string) int {
	var claimedOffset int
	success := false
	for !success {
		success = true
		prev, err := kn.kv.ReadInt(ctx, latestOffsetKey(key))
		if err != nil {
			prev = -1
		}
		claimedOffset = prev + 1
		if err := kn.kv.CompareAndSwap(ctx, latestOffsetKey(key), prev, prev+1, true); err != nil {
			success = false
		}
	}
	return claimedOffset
}

func (kn *KafkaNode) sendHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	ctx := context.Background()
	key := body["key"].(string)
	newMsg := int(body["msg"].(float64))

	offset := kn.claimNextOffset(ctx, key)
	if err := kn.kv.Write(ctx, offsetKey(key, offset), newMsg); err != nil {
		return err
	}

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
	ctx := context.Background()

	msgs := make(map[string][][]int)
	for key, offset := range body.Offsets {
		msgs[key] = [][]int{}
		for {
			msg, err := kn.kv.ReadInt(ctx, offsetKey(key, int(offset)))
			if err != nil {
				break
			}
			msgs[key] = append(msgs[key], []int{int(offset), msg})
			offset++
		}
	}

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
	ctx := context.Background()

	for key, offset := range body.Offsets {
		if err := kn.kv.Write(ctx, committedOffsetKey(key), offset); err != nil {
			return err
		}
	}

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
	ctx := context.Background()

	offsets := make(map[string]int)
	for _, key := range body.Keys {
		offset, err := kn.kv.ReadInt(ctx, committedOffsetKey(key))
		if err != nil {
			offset = 0
		}
		offsets[key] = offset
	}

	res := make(map[string]any)
	res["type"] = "list_committed_offsets_ok"
	res["offsets"] = offsets
	return kn.n.Reply(msg, res)
}

func main() {
	node := maelstrom.NewNode()
	kv := maelstrom.NewLinKV(node)
	kn := KafkaNode{
		n:  node,
		kv: kv,
	}

	kn.n.Handle("send", kn.sendHandler)
	kn.n.Handle("poll", kn.pollHandler)
	kn.n.Handle("commit_offsets", kn.committedOffsetsHandler)
	kn.n.Handle("list_committed_offsets", kn.listCommittedOffsetsHandler)

	if err := kn.n.Run(); err != nil {
		log.Fatal(err)
	}
}
