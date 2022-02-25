package flame

import (
	"sort"
	"sync"

	"golang.org/x/exp/constraints"
)

type Workflow struct {
	WaitGroup    *sync.WaitGroup
	Nodes        []Process
}

type KeyValue[K constraints.Ordered, V any] struct {
	Key   K
	Value V
}

type Node[X, Y any] interface {
	AddInput(i chan X)
	GetOutput() chan Y
	Connect(e Emitter[X])
}

type Emitter[X any] interface {
	GetOutput() chan X
}

type Process interface {
	Init(wf *Workflow)
}

func NewWorkflow() *Workflow {
	return &Workflow{}
}

func (wf *Workflow) Init() {
	wf.WaitGroup = &sync.WaitGroup{}
	for i := range wf.Nodes {
		wf.Nodes[i].Init(wf)
	}
}

func (wf *Workflow) Wait() {
	wf.WaitGroup.Wait()
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

func (n *MapNode[X, Y]) Init(wf *Workflow) {
	wf.WaitGroup.Add(1)
	go func() {
		for x := range n.Input {
			y := n.Proc(x)
			for i := range n.Outputs {
				n.Outputs[i] <- y
			}
		}
		for i := range n.Outputs {
			close(n.Outputs[i])
		}
		wf.WaitGroup.Done()
	}()
}

func (n *MapNode[X, Y]) AddInput(i chan X) {
	n.Input = i
}

func (n *MapNode[X, Y]) GetOutput() chan Y {
	m := make(chan Y)
	n.Outputs = append(n.Outputs, m)
	return m
}

func (n *MapNode[X, Y]) Connect(e Emitter[X]) {
	o := e.GetOutput()
	n.AddInput(o)
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

func (s *SortNode[X, Y]) Init(wf *Workflow) {
	wf.WaitGroup.Add(1)
	go func() {
		for x := range s.Input {
			s.Queue = append(s.Queue, x)
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

func (n *SortNode[X, Y]) AddInput(i chan KeyValue[X, Y]) {
	n.Input = i
}

func (n *SortNode[X, Y]) Connect(e Emitter[KeyValue[X, Y]]) {
	o := e.GetOutput()
	n.AddInput(o)
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
	Start   Y
	Input   chan X
	Outputs []chan Y
	Proc    func(X, Y) Y
}

func AddReducer[X, Y any](w *Workflow, f func(X, Y) Y, start Y) Node[X, Y] {
	n := &ReduceNode[X, Y]{Proc: f, Outputs: []chan Y{}, Start: start}
	w.Nodes = append(w.Nodes, n)
	return n
}

func (n *ReduceNode[X, Y]) Init(wf *Workflow) {
	wf.WaitGroup.Add(1)
	go func() {
		y := n.Start
		for x := range n.Input {
			y = n.Proc(x, y)
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

func (n *ReduceNode[X, Y]) AddInput(i chan X) {
	n.Input = i
}

func (n *ReduceNode[X, Y]) GetOutput() chan Y {
	m := make(chan Y)
	n.Outputs = append(n.Outputs, m)
	return m
}

func (n *ReduceNode[X, Y]) Connect(e Emitter[X]) {
	o := e.GetOutput()
	n.AddInput(o)
}
