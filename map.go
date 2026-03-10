// Package flame provides a lightweight, generic flow‑processing framework.
// A Node represents a step in a directed acyclic graph; each node consumes a channel of inputs and emits one or more output channels.
// The framework is deliberately minimal – it only coordinates goroutine launch, channel wiring and WaitGroup tracking.

package flame

/**************************/
// Mapper
/**************************/

// MapNode represents a flow step that takes an input X calls a function f(X) that
// returns Y
// MapNode represents a flow step that transforms values of type X into values
// of type Y using the provided function `Proc`. It receives input values on the
// `Input` channel and forwards the transformed results to all registered output
// channels.

type MapNode[X, Y any] struct {
	Input   chan X
	Outputs []chan Y
	Proc    func(X) Y
}

// AddMapper adds a MapNode step to the workflow
// AddMapper adds a MapNode step to the workflow. The supplied function `f` is applied to every element that arrives on the input channel. The returned node can be connected to an upstream emitter and its output retrieved via GetOutput.
func AddMapper[X, Y any](w *Workflow, f func(X) Y) Node[X, Y] {
	n := &MapNode[X, Y]{Proc: f, Outputs: []chan Y{}}
	w.Nodes = append(w.Nodes, n)
	return n
}

// Start function called by Workflow object
func (n *MapNode[X, Y]) start(wf *Workflow) {
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

// GetOutput returns a chan Y of the results of f(Y)
// This channel must be read out or the flow will stop
func (n *MapNode[X, Y]) GetOutput() chan Y {
	m := make(chan Y)
	n.Outputs = append(n.Outputs, m)
	return m
}

// Connect sets the input X stream for f(X)
func (n *MapNode[X, Y]) Connect(e Emitter[X]) {
	o := e.GetOutput()
	n.Input = o
}
