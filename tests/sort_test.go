package tests

import (
	"fmt"
	"github.com/bmeg/flame"
	"testing"
)

func KeyInc(x flame.KeyValue[int, string]) flame.KeyValue[int, string] {
	x.Key += 1
	return x
}

func TestIntSort(t *testing.T) {
	in := make(chan flame.KeyValue[int, string], 10)

	wf := flame.NewWorkflow()
	a := flame.AddMapper(wf, KeyInc)
	a.AddInput(in)

	b := flame.AddKeySort[int, string](wf)
	b.Connect(a)
	out1 := b.GetOutput()
	wf.Init()

	v := []flame.KeyValue[int, string]{
		{3, "charles"},
		{2, "bob"},
		{1, "alice"},
		{4, "dan"},
		{5, "edward"},
	}

	go func() {
		for _, n := range v {
			in <- n
		}
		close(in)
	}()

	for y := range out1 {
		fmt.Printf("%d %s\n", y.Key, y.Value)
	}
}

func TestStringSort(t *testing.T) {
	in := make(chan flame.KeyValue[string, int], 10)

	wf := flame.NewWorkflow()
	a := flame.AddKeySort[string, int](wf)
	a.AddInput(in)
	out1 := a.GetOutput()

	wf.Init()

	v := []flame.KeyValue[string, int]{
		{"charles", 3},
		{"bob", 2},
		{"alice", 1},
		{"dan", 4},
		{"edward", 5},
	}

	go func() {
		for _, n := range v {
			in <- n
		}
		close(in)
	}()

	for y := range out1 {
		fmt.Printf("%s %d\n", y.Key, y.Value)
	}
}
