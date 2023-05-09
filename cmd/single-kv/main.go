package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type KVNode struct {
	n     *maelstrom.Node
	kv    map[int]int
	mutex sync.Mutex
}

type txnMsg struct {
	Txn [][3]interface{} `json:"txn"`
}

func (kn *KVNode) transactionHandler(msg maelstrom.Message) error {
	var body txnMsg
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	kn.mutex.Lock()

	for i, op := range body.Txn {
		if op[0] == "r" {
			key := int(op[1].(float64))
			if val, ok := kn.kv[key]; ok {
				body.Txn[i][2] = val
			}
		} else {
			key := int(op[1].(float64))
			val := int(op[2].(float64))
			kn.kv[key] = val
		}

	}

	kn.mutex.Unlock()

	res := make(map[string]any)
	res["type"] = "txn_ok"
	res["txn"] = body.Txn
	return kn.n.Reply(msg, res)
}

func main() {
	kn := KVNode{
		n:  maelstrom.NewNode(),
		kv: make(map[int]int),
	}

	kn.n.Handle("txn", kn.transactionHandler)

	if err := kn.n.Run(); err != nil {
		log.Fatal(err)
	}
}
