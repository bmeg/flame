package flame

import (
	"io/ioutil"
	"path/filepath"
	"sync"

	"golang.org/x/exp/constraints"
)

type Workflow struct {
	WaitGroup *sync.WaitGroup
	Nodes     []Process
	WorkDir   string
}

type KeyValue[K constraints.Ordered, V any] struct {
	Key   K
	Value V
}

type Node[X, Y any] interface {
	GetOutput() chan Y
	Connect(e Emitter[X])
}

type Receiver[X any] interface {
	Connect(e Emitter[X])
}

type Emitter[X any] interface {
	GetOutput() chan X
}

type Process interface {
	Start(wf *Workflow)
}

func NewWorkflow() *Workflow {
	return &Workflow{}
}

func (wf *Workflow) Start() {
	wf.WaitGroup = &sync.WaitGroup{}
	for i := range wf.Nodes {
		wf.Nodes[i].Start(wf)
	}
}

func (wf *Workflow) SetWorkDir(path string) {
	wf.WorkDir, _ = filepath.Abs(path)
}

func (wf *Workflow) Wait() {
	wf.WaitGroup.Wait()
}

func (wf *Workflow) GetTmpDir() (string, error) {
	return ioutil.TempDir(wf.WorkDir, "flame_")
}

/**************************/
// Source Chan
/**************************/

type SourceChanNode[X, Y any] struct {
	Source  chan Y
	Outputs []chan Y
}

func AddSourceChan[Y any](w *Workflow, i chan Y) Node[any, Y] {
	n := &SourceChanNode[any, Y]{Source: i, Outputs: []chan Y{}}
	w.Nodes = append(w.Nodes, n)
	return n
}

func (n *SourceChanNode[X, Y]) Connect(e Emitter[X]) {
	//this should throw an error
}

func (n *SourceChanNode[X, Y]) Start(wf *Workflow) {
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

func (n *SourceChanNode[X, Y]) GetOutput() chan Y {
	m := make(chan Y)
	n.Outputs = append(n.Outputs, m)
	return m
}

/**************************/
// Source
/**************************/

type SourceNode[X, Y any] struct {
	Source  func() (Y, error)
	Outputs []chan Y
}

func AddSource[Y any](w *Workflow, i func() (Y, error)) Node[any, Y] {
	n := &SourceNode[any, Y]{Source: i, Outputs: []chan Y{}}
	w.Nodes = append(w.Nodes, n)
	return n
}

func (n *SourceNode[X, Y]) Connect(e Emitter[X]) {
	//this should throw an error
}

func (n *SourceNode[X, Y]) Start(wf *Workflow) {
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

func (n *SourceNode[X, Y]) GetOutput() chan Y {
	m := make(chan Y)
	n.Outputs = append(n.Outputs, m)
	return m
}

/**************************/
// Sink
/**************************/

type SinkNode[X, Y any] struct {
	Input chan X
	Sink  func(X)
}

func AddSink[X any](w *Workflow, i func(X)) Node[X, any] {
	n := &SinkNode[X, any]{Sink: i}
	w.Nodes = append(w.Nodes, n)
	return n
}

func (n *SinkNode[X, Y]) Connect(e Emitter[X]) {
	o := e.GetOutput()
	n.Input = o
}

func (n *SinkNode[X, Y]) Start(wf *Workflow) {
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

func (n *SinkNode[X, Y]) GetOutput() chan Y {
	return nil
}
