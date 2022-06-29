package flame

/**************************/
// Reducer
/**************************/

type ReduceNode[X, Y any] struct {
	Init    Y
	Input   chan X
	Outputs []chan Y
	Proc    func(X, Y) Y
}

func AddReducer[X, Y any](w *Workflow, f func(X, Y) Y, init Y) Node[X, Y] {
	n := &ReduceNode[X, Y]{Proc: f, Outputs: []chan Y{}, Init: init}
	w.Nodes = append(w.Nodes, n)
	return n
}

func (n *ReduceNode[X, Y]) Start(wf *Workflow) {
	wf.WaitGroup.Add(1)
	go func() {
		y := n.Init
		if n.Input != nil {
			for x := range n.Input {
				y = n.Proc(x, y)
			}
		}
		for i := range n.Outputs {
			n.Outputs[i] <- y
		}
		for i := range n.Outputs {
			close(n.Outputs[i])
		}
		wf.WaitGroup.Done()
	}()
}

func (n *ReduceNode[X, Y]) GetOutput() chan Y {
	m := make(chan Y)
	n.Outputs = append(n.Outputs, m)
	return m
}

func (n *ReduceNode[X, Y]) Connect(e Emitter[X]) {
	o := e.GetOutput()
	n.Input = o
}
