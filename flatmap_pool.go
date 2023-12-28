package flame

/**************************/
// FlatMapper
/**************************/

type FlatMapPoolNode[X, Y any] struct {
	Input       chan X
	Outputs     []chan Y
	Proc        func(X) []Y
	ProcCount   int
	ChannelSize int
}

func AddFlatMapperPool[X, Y any](w *Workflow, f func(X) []Y, nthread int) Node[X, Y] {
	n := &FlatMapPoolNode[X, Y]{Proc: f, Outputs: []chan Y{}, ProcCount: nthread}
	w.Nodes = append(w.Nodes, n)
	return n
}

func (n *FlatMapPoolNode[X, Y]) start(wf *Workflow) {
	wf.WaitGroup.Add(1)

	if n.ChannelSize <= 0 {
		n.ChannelSize = 1
	}

	wkrInputs := make([]chan X, n.ProcCount)
	wkrOutputs := make([]chan []Y, n.ProcCount)

	//worker pool
	for i := 0; i < n.ProcCount; i++ {
		wkrInputs[i] = make(chan X, n.ChannelSize)
		wkrOutputs[i] = make(chan []Y, n.ChannelSize)
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
					for _, z := range y {
						n.Outputs[i] <- z
					}
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

func (n *FlatMapPoolNode[X, Y]) GetOutput() chan Y {
	m := make(chan Y)
	n.Outputs = append(n.Outputs, m)
	return m
}

func (n *FlatMapPoolNode[X, Y]) Connect(e Emitter[X]) {
	o := e.GetOutput()
	n.Input = o
}
