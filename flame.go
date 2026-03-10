// Package flame provides a lightweight, generic flow‑processing framework.
// It offers building blocks (nodes) for constructing concurrent data pipelines
// using Go generics. Nodes are wired together via channels and executed in goroutines
// managed by a Workflow.

package flame

import (
	"os"
	"path/filepath"
	"sync"

	"golang.org/x/exp/constraints"
)

// Workflow represents a collection of processing nodes that will be executed concurrently.
// It holds a WaitGroup to synchronize node goroutines and an optional working directory.
type Workflow struct {
	WaitGroup *sync.WaitGroup
	Nodes     []Process
	WorkDir   string
}

// KeyValue is a generic pair used by keyed operations (e.g., join, accumulate).
// K must be an ordered type, V can be any type.
type KeyValue[K constraints.Ordered, V any] struct {
	Key   K
	Value V
}

// Node represents a processing element that can be connected to an upstream emitter and produce an output channel.
// X is the input type, Y is the output type.
type Node[X, Y any] interface {
	GetOutput() chan Y
	Connect(e Emitter[X])
}

// Receiver is a node that only consumes input; it does not produce output.
// It is used for sink‑like nodes.
type Receiver[X any] interface {
	Connect(e Emitter[X])
}

// Emitter produces a channel of values of type X.
type Emitter[X any] interface {
	GetOutput() chan X
}

// Process is an internal interface implemented by all node types; start launches the node.
type Process interface {
	start(wf *Workflow)
}

// NewWorkflow constructs an empty workflow ready for nodes to be added.
func NewWorkflow() *Workflow {
	return &Workflow{}
}

// Start initializes the WaitGroup and launches all nodes in the workflow.
func (wf *Workflow) Start() {
	wf.WaitGroup = &sync.WaitGroup{}
	for i := range wf.Nodes {
		wf.Nodes[i].start(wf)
	}
}

// SetWorkDir sets the working directory for the workflow, converting it to an absolute path.
func (wf *Workflow) SetWorkDir(path string) {
	wf.WorkDir, _ = filepath.Abs(path)
}

// Wait blocks until all node goroutines have completed.
func (wf *Workflow) Wait() {
	wf.WaitGroup.Wait()
}

// GetTmpDir creates a temporary directory inside the workflow's working directory.
func (wf *Workflow) GetTmpDir() (string, error) {
	return os.MkdirTemp(wf.WorkDir, "flame_")
}

// SourceChanNode receives values from a user‑provided channel and forwards them downstream.
type SourceChanNode[X, Y any] struct {
	Source  chan Y
	Outputs []chan Y
}

// AddSourceChan adds a source node that reads from the supplied channel `i`.
// It returns a Node that can be connected to downstream processors.
func AddSourceChan[Y any](w *Workflow, i chan Y) Node[any, Y] {
	n := &SourceChanNode[any, Y]{Source: i, Outputs: []chan Y{}}
	w.Nodes = append(w.Nodes, n)
	return n
}

// Connect is a no‑op for SourceChanNode because it is a source; attempting to connect will panic.
func (n *SourceChanNode[X, Y]) Connect(e Emitter[X]) {
	panic("cannot connect source node")
}

// start launches a goroutine that forwards values from the source channel to all outputs.
func (n *SourceChanNode[X, Y]) start(wf *Workflow) {
	wf.WaitGroup.Add(1)
	go func() {
		for x := range n.Source {
			for i := range n.Outputs {
				n.Outputs[i] <- x
			}
		}
		for i := range n.Outputs {
			close(n.Outputs[i])
		}
		wf.WaitGroup.Done()
	}()
}

// GetOutput creates a new output channel for downstream nodes.
func (n *SourceChanNode[X, Y]) GetOutput() chan Y {
	m := make(chan Y)
	n.Outputs = append(n.Outputs, m)
	return m
}

// SourceNode creates values by calling a user‑provided function that returns (Y, error).
// The node stops when the function returns an error.
type SourceNode[X, Y any] struct {
	Source  func() (Y, error)
	Outputs []chan Y
}

// AddSource adds a source node that invokes the supplied function to produce values.
func AddSource[Y any](w *Workflow, i func() (Y, error)) Node[any, Y] {
	n := &SourceNode[any, Y]{Source: i, Outputs: []chan Y{}}
	w.Nodes = append(w.Nodes, n)
	return n
}

// Connect is a no‑op for SourceNode; sources cannot be wired upstream.
func (n *SourceNode[X, Y]) Connect(e Emitter[X]) {
	panic("cannot connect source node")
}

// start runs the source function repeatedly until it returns an error, forwarding each value.
func (n *SourceNode[X, Y]) start(wf *Workflow) {
	wf.WaitGroup.Add(1)
	go func() {
		for {
			x, err := n.Source()
			if err != nil {
				break
			}
			for i := range n.Outputs {
				n.Outputs[i] <- x
			}
		}
		for i := range n.Outputs {
			close(n.Outputs[i])
		}
		wf.WaitGroup.Done()
	}()
}

// GetOutput creates and registers an output channel for downstream nodes.
func (n *SourceNode[X, Y]) GetOutput() chan Y {
	m := make(chan Y)
	n.Outputs = append(n.Outputs, m)
	return m
}

// SinkNode consumes input values and passes them to a user‑provided sink function.
type SinkNode[X, Y any] struct {
	Input chan X
	Sink  func(X)
}

// AddSink adds a sink node that calls the provided function `i` for each received value.
func AddSink[X any](w *Workflow, i func(X)) Node[X, any] {
	n := &SinkNode[X, any]{Sink: i}
	w.Nodes = append(w.Nodes, n)
	return n
}

// Connect wires the sink's input to the output of an upstream emitter.
func (n *SinkNode[X, Y]) Connect(e Emitter[X]) {
	o := e.GetOutput()
	n.Input = o
}

// start launches a goroutine that reads from the input channel and calls the sink function.
func (n *SinkNode[X, Y]) start(wf *Workflow) {
	wf.WaitGroup.Add(1)
	go func() {
		if n.Input != nil {
			for x := range n.Input {
				n.Sink(x)
			}
		}
		wf.WaitGroup.Done()
	}()
}

// GetOutput returns nil because sink nodes do not produce downstream output.
func (n *SinkNode[X, Y]) GetOutput() chan Y {
	return nil
}
