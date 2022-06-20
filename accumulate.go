package flame

import "golang.org/x/exp/constraints"

type AccumulateNode[K constraints.Ordered, X, Y any] struct {
	Init    Y
	Input   chan KeyValue[K, X]
	Outputs []chan KeyValue[K, Y]
	Proc    func(K, []X) Y
}

func AddAccumulate[K constraints.Ordered, X, Y any](w *Workflow, f func(K, []X) Y) Node[KeyValue[K, X], KeyValue[K, Y]] {
	n := &AccumulateNode[K, X, Y]{Proc: f, Outputs: []chan KeyValue[K, Y]{}}
	w.Nodes = append(w.Nodes, n)
	return n
}

func (n *AccumulateNode[K, X, Y]) Connect(e Emitter[KeyValue[K, X]]) {
	o := e.GetOutput()
	n.Input = o
}

func (n *AccumulateNode[K, X, Y]) GetOutput() chan KeyValue[K, Y] {
	m := make(chan KeyValue[K, Y])
	n.Outputs = append(n.Outputs, m)
	return m
}

func (n *AccumulateNode[K, X, Y]) Start(wf *Workflow) {
	wf.WaitGroup.Add(1)
	go func() {
		first := true
		var key K
		data := []X{}
		for x := range n.Input {
			if x.Key != key {
				if !first {
					res := n.Proc(key, data)
					for i := range n.Outputs {
						n.Outputs[i] <- KeyValue[K, Y]{key, res}
					}
				} else {
					first = false
				}
				key = x.Key
				data = []X{}
			}
			data = append(data, x.Value)
		}

		res := n.Proc(key, data)
		for i := range n.Outputs {
			n.Outputs[i] <- KeyValue[K, Y]{key, res}
		}

		for i := range n.Outputs {
			close(n.Outputs[i])
		}
		wf.WaitGroup.Done()
	}()
}
