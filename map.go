package flame


/**************************/
// Mapper
/**************************/

type MapNode[X, Y any] struct {
	Input   chan X
	Outputs []chan Y
	Proc    func(X) Y
}

func AddMapper[X, Y any](w *Workflow, f func(X) Y) Node[X, Y] {
	n := &MapNode[X, Y]{Proc: f, Outputs: []chan Y{}}
	w.Nodes = append(w.Nodes, n)
	return n
}

func (n *MapNode[X, Y]) Start(wf *Workflow) {
	wf.WaitGroup.Add(1)
	go func() {
		if n.Input != nil {
			for x := range n.Input {
				y := n.Proc(x)
				for i := range n.Outputs {
					n.Outputs[i] <- y
				}
			}
		}
		for i := range n.Outputs {
			close(n.Outputs[i])
		}
		wf.WaitGroup.Done()
	}()
}

func (n *MapNode[X, Y]) GetOutput() chan Y {
	m := make(chan Y)
	n.Outputs = append(n.Outputs, m)
	return m
}

func (n *MapNode[X, Y]) Connect(e Emitter[X]) {
	o := e.GetOutput()
	n.Input = o
}
