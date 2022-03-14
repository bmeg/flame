package flame

import (
	"io/ioutil"
	"path/filepath"
	"sort"
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

/**************************/
// FlatMapper
/**************************/

type FlatMapNode[X, Y any] struct {
	Input   chan X
	Outputs []chan Y
	Proc    func(X) []Y
}

func AddFlatMapper[X, Y any](w *Workflow, f func(X) []Y) Node[X, Y] {
	n := &FlatMapNode[X, Y]{Proc: f, Outputs: []chan Y{}}
	w.Nodes = append(w.Nodes, n)
	return n
}

func (n *FlatMapNode[X, Y]) Start(wf *Workflow) {
	wf.WaitGroup.Add(1)
	go func() {
		if n.Input != nil {
			for x := range n.Input {
				y := n.Proc(x)
				for i := range n.Outputs {
					for _, z := range y {
						n.Outputs[i] <- z
					}
				}
			}
		}
		for i := range n.Outputs {
			close(n.Outputs[i])
		}
		wf.WaitGroup.Done()
	}()
}

func (n *FlatMapNode[X, Y]) GetOutput() chan Y {
	m := make(chan Y)
	n.Outputs = append(n.Outputs, m)
	return m
}

func (n *FlatMapNode[X, Y]) Connect(e Emitter[X]) {
	o := e.GetOutput()
	n.Input = o
}

/**************************/
// Sorter
/**************************/

func AddKeySort[X constraints.Ordered, Y any](w *Workflow) Node[KeyValue[X, Y], KeyValue[X, Y]] {
	q := make([]KeyValue[X, Y], 0, 10)
	n := &SortNode[X, Y]{Queue: q}
	w.Nodes = append(w.Nodes, n)
	return n
}

type SortNode[X constraints.Ordered, Y any] struct {
	Input   chan KeyValue[X, Y]
	Queue   []KeyValue[X, Y]
	Outputs []chan KeyValue[X, Y]
}

// Swap is part of sort.Interface.
func (s *SortNode[X, Y]) Swap(i, j int) {
	s.Queue[i], s.Queue[j] = s.Queue[j], s.Queue[i]
}

// Less is part of sort.Interface. It is implemented by calling the "by" closure in the sorter.
func (s *SortNode[X, Y]) Less(i, j int) bool {
	return s.Queue[i].Key < s.Queue[j].Key
}

// Len is part of sort.Interface.
func (s *SortNode[X, Y]) Len() int {
	return len(s.Queue)
}

func (s *SortNode[X, Y]) Start(wf *Workflow) {
	wf.WaitGroup.Add(1)
	go func() {
		if s.Input != nil {
			for x := range s.Input {
				s.Queue = append(s.Queue, x)
			}
		}
		sort.Sort(s)
		for _, y := range s.Queue {
			for i := range s.Outputs {
				s.Outputs[i] <- y
			}
		}
		for i := range s.Outputs {
			close(s.Outputs[i])
		}
		wf.WaitGroup.Done()
	}()
}

func (n *SortNode[X, Y]) Connect(e Emitter[KeyValue[X, Y]]) {
	o := e.GetOutput()
	n.Input = o
}

func (n *SortNode[X, Y]) GetOutput() chan KeyValue[X, Y] {
	m := make(chan KeyValue[X, Y])
	n.Outputs = append(n.Outputs, m)
	return m
}

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
