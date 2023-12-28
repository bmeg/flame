package flame

/**************************/
// Map Pool
/**************************/

type MapPoolNode[X, Y any] struct {
	Input       chan X
	Outputs     []chan Y
	Proc        func(X) Y
	ProcCount   int
	ChannelSize int
}

// AddMapperPool adds a map step to the flow. As opposed to MapNode, MapPoolNode uses
// a pool of N workers to process elements in parallel.
func AddMapperPool[X, Y any](w *Workflow, f func(X) Y, nthread int) Node[X, Y] {
	n := &MapPoolNode[X, Y]{Proc: f, Outputs: []chan Y{}, ProcCount: nthread, ChannelSize: 10}
	w.Nodes = append(w.Nodes, n)
	return n
}

func (n *MapPoolNode[X, Y]) start(wf *Workflow) {
	wf.WaitGroup.Add(1)

	if n.ChannelSize <= 0 {
		n.ChannelSize = 1
	}

	wkrInputs := make([]chan X, n.ProcCount)
	wkrOutputs := make([]chan Y, n.ProcCount)

	//worker pool
	for i := 0; i < n.ProcCount; i++ {
		wkrInputs[i] = make(chan X, n.ChannelSize)
		wkrOutputs[i] = make(chan Y, n.ChannelSize)
		go func(wNum int) {
			if n.Input != nil {
				for x := range wkrInputs[wNum] {
					y := n.Proc(x)
					wkrOutputs[wNum] <- y
				}
			}
			close(wkrOutputs[wNum])
		}(i)
	}

	//spread inputs
	go func() {
		if n.Input != nil {
			w := 0
			for x := range n.Input {
				wkrInputs[w] <- x
				w = (w + 1) % n.ProcCount
			}
			for i := 0; i < n.ProcCount; i++ {
				close(wkrInputs[i])
			}
		}
	}()

	//collect outputs
	go func() {
		w := 0
		active := make([]bool, n.ProcCount)
		for i := 0; i < n.ProcCount; i++ {
			active[i] = true
		}
		for anyTrue(active) {
			if y, ok := <-wkrOutputs[w]; ok {
				for i := range n.Outputs {
					n.Outputs[i] <- y
				}
			} else {
				active[w] = false
			}
			w = (w + 1) % n.ProcCount
		}
		for i := range n.Outputs {
			close(n.Outputs[i])
		}
		wf.WaitGroup.Done()
	}()

}

func (n *MapPoolNode[X, Y]) GetOutput() chan Y {
	m := make(chan Y)
	n.Outputs = append(n.Outputs, m)
	return m
}

func (n *MapPoolNode[X, Y]) Connect(e Emitter[X]) {
	o := e.GetOutput()
	n.Input = o
}
