package flame

/**************************/
// Stream
/**************************/

type StreamNode[X, Y any] struct {
	Input   chan X
	Outputs []chan Y
	Proc    func(chan X, chan Y)
}

func AddStreamer[X, Y any](w *Workflow, f func(chan X, chan Y)) Node[X, Y] {
	n := &StreamNode[X, Y]{Proc: f, Outputs: []chan Y{}}
	w.Nodes = append(w.Nodes, n)
	return n
}

func (n *StreamNode[X, Y]) Start(wf *Workflow) {
	wf.WaitGroup.Add(1)
	out := make(chan Y, 10)
	go func() {
		if n.Input != nil {
			n.Proc(n.Input, out)
		}
		close(out)
	}()
	go func() {
		for o := range out {
			for i := range n.Outputs {
				n.Outputs[i] <- o
			}
		}
		for i := range n.Outputs {
			close(n.Outputs[i])
		}
		wf.WaitGroup.Done()
	}()
}

func (n *StreamNode[X, Y]) GetOutput() chan Y {
	m := make(chan Y)
	n.Outputs = append(n.Outputs, m)
	return m
}

func (n *StreamNode[X, Y]) Connect(e Emitter[X]) {
	o := e.GetOutput()
	n.Input = o
}
