package flame

import (
	"sort"

	"golang.org/x/exp/constraints"
)

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
