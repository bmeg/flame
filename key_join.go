package flame

import (
	"sync"

	"golang.org/x/exp/constraints"
)

type KeyJoinNode[K constraints.Ordered, X, Y, Z any] struct {
	LeftInput  chan KeyValue[K, X]
	RightInput chan KeyValue[K, Y]
	Outputs    []chan KeyValue[K, Z]
	Proc       func(K, []X, []Y) Z
}

func AddKeyJoin[K constraints.Ordered, X, Y, Z any](w *Workflow, f func(K, []X, []Y) Z) *KeyJoinNode[K, X, Y, Z] {
	n := &KeyJoinNode[K, X, Y, Z]{Proc: f, Outputs: []chan KeyValue[K, Z]{}}
	w.Nodes = append(w.Nodes, n)
	return n
}

func (n *KeyJoinNode[K, X, Y, Z]) GetOutput() chan KeyValue[K, Z] {
	m := make(chan KeyValue[K, Z])
	n.Outputs = append(n.Outputs, m)
	return m
}

func (n *KeyJoinNode[K, X, Y, Z]) ConnectLeft(e Emitter[KeyValue[K, X]]) {
	o := e.GetOutput()
	n.LeftInput = o
}

func (n *KeyJoinNode[K, X, Y, Z]) ConnectRight(e Emitter[KeyValue[K, Y]]) {
	o := e.GetOutput()
	n.RightInput = o
}

func (n *KeyJoinNode[K, X, Y, Z]) Start(wf *Workflow) {
	wf.WaitGroup.Add(1)

	wg := &sync.WaitGroup{}
	wg.Add(2)
	left := map[K][]X{}
	right := map[K][]Y{}
	go func() {
		for i := range n.LeftInput {
			if x, ok := left[i.Key]; ok {
				left[i.Key] = append(x, i.Value)
			} else {
				left[i.Key] = []X{i.Value}
			}
		}
		wg.Done()
	}()
	go func() {
		for i := range n.RightInput {
			if x, ok := right[i.Key]; ok {
				right[i.Key] = append(x, i.Value)
			} else {
				right[i.Key] = []Y{i.Value}
			}
		}
		wg.Done()
	}()

	go func() {
		wg.Wait()

		for key, lVals := range left {
			if rVals, ok := right[key]; ok {
				for i := range n.Outputs {
					n.Outputs[i] <- KeyValue[K, Z]{key, n.Proc(key, lVals, rVals)}
				}
			}
		}

		for i := range n.Outputs {
			close(n.Outputs[i])
		}
		wf.WaitGroup.Done()
	}()

}

type KeySort[X constraints.Ordered, Y any] []KeyValue[X, Y]

// Swap is part of sort.Interface.
func (s KeySort[X, Y]) Swap(i, j int) {
	(s)[i], (s)[j] = (s)[j], (s)[i]
}

// Less is part of sort.Interface. It is implemented by calling the "by" closure in the sorter.
func (s KeySort[X, Y]) Less(i, j int) bool {
	return (s)[i].Key < (s)[j].Key
}

// Len is part of sort.Interface.
func (s KeySort[X, Y]) Len() int {
	return len(s)
}
