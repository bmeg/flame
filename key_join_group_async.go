package flame

import (
	"sync"

	"golang.org/x/exp/constraints"
)

type KeyJoinGroupAsyncNode[K constraints.Ordered, X, Z any] struct {
	Inputs  []chan KeyValue[K, X]
	Outputs []chan KeyValue[K, Z]
	Proc    func(K, []X) Z
}

func AddKeyJoinGroupAsync[K constraints.Ordered, X, Z any](w *Workflow, f func(K, []X) Z) *KeyJoinGroupAsyncNode[K, X, Z] {
	n := &KeyJoinGroupAsyncNode[K, X, Z]{Proc: f, Outputs: []chan KeyValue[K, Z]{}}
	w.Nodes = append(w.Nodes, n)
	return n
}

func (n *KeyJoinGroupAsyncNode[K, X, Z]) Start(wf *Workflow) {
	wf.WaitGroup.Add(1)

	mut := &sync.Mutex{}
	store := map[K][]X{}

	updates := make(chan K, len(n.Inputs))

	wg := &sync.WaitGroup{}
	for i := 0; i < len(n.Inputs); i++ {
		wg.Add(1)
		go func(inputN int) {
			for l := range n.Inputs[inputN] {
				mut.Lock()
				if v, ok := store[l.Key]; ok {
					v[inputN] = l.Value
				} else {
					v := make([]X, len(n.Inputs))
					v[inputN] = l.Value
					store[l.Key] = v
				}
				mut.Unlock()
				updates <- l.Key
			}
			wg.Done()
		}(i)
	}

	go func() {
		wg.Wait()
		close(updates)
	}()

	go func() {
		hits := map[K]int{}
		for key := range updates {
			if x, ok := hits[key]; ok {
				x++
				if x >= len(n.Inputs) {
					delete(hits, key)
					mut.Lock()
					val := n.Proc(key, store[key])
					delete(store, key)
					mut.Unlock()
					for i := range n.Outputs {
						n.Outputs[i] <- KeyValue[K, Z]{key, val}
					}
				} else {
					hits[key] = x
				}
			} else {
				hits[key] = 1
			}
		}
		for i := range n.Outputs {
			close(n.Outputs[i])
		}
		wf.WaitGroup.Done()
	}()
}

func (n *KeyJoinGroupAsyncNode[K, X, Z]) GetOutput() chan KeyValue[K, Z] {
	m := make(chan KeyValue[K, Z])
	n.Outputs = append(n.Outputs, m)
	return m
}

func (n *KeyJoinGroupAsyncNode[K, X, Z]) Connect(e Emitter[KeyValue[K, X]]) {
	o := e.GetOutput()
	n.Inputs = append(n.Inputs, o)
}
