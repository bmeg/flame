package flame

type JoinNode[X, Y, Z any] struct {
	LeftInput  chan X
	RightInput chan Y
	Outputs    []chan Z
	Proc       func(chan X, chan Y, chan Z)
}

func AddJoin[X, Y, Z any](w *Workflow, f func(chan X, chan Y, chan Z)) *JoinNode[X, Y, Z] {
	n := &JoinNode[X, Y, Z]{Proc: f, Outputs: []chan Z{}}
	w.Nodes = append(w.Nodes, n)
	return n
}

func (n *JoinNode[X, Y, Z]) GetOutput() chan Z {
	m := make(chan Z)
	n.Outputs = append(n.Outputs, m)
	return m
}

func (n *JoinNode[X, Y, Z]) ConnectLeft(e Emitter[X]) {
	o := e.GetOutput()
	n.LeftInput = o
}

func (n *JoinNode[X, Y, Z]) ConnectRight(e Emitter[Y]) {
	o := e.GetOutput()
	n.RightInput = o
}

func (n *JoinNode[X, Y, Z]) Start(wf *Workflow) {
	wf.WaitGroup.Add(1)
	out := make(chan Z, 10)
	go func() {
		if n.LeftInput != nil && n.RightInput != nil {
			n.Proc(n.LeftInput, n.RightInput, out)
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
