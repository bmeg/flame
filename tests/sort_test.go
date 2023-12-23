package tests

import (
	"fmt"
	"testing"

	"github.com/bmeg/flame"
)

func KeyInc(x flame.KeyValue[int, string]) flame.KeyValue[int, string] {
	x.Key += 1
	return x
}

func TestIntSort(t *testing.T) {
	in := make(chan flame.KeyValue[int, string], 10)

	wf := flame.NewWorkflow()
	inc := flame.AddSourceChan(wf, in)
	a := flame.AddMapper(wf, KeyInc)
	a.Connect(inc)

	b := flame.AddKeySort[int, string](wf)
	b.Connect(a)
	out1 := b.GetOutput()
	wf.Start()

	v := []flame.KeyValue[int, string]{
		{Key: 3, Value: "charles"},
		{Key: 2, Value: "bob"},
		{Key: 1, Value: "alice"},
		{Key: 4, Value: "dan"},
		{Key: 5, Value: "edward"},
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
	inc := flame.AddSourceChan(wf, in)
	a := flame.AddKeySort[string, int](wf)
	a.Connect(inc)
	out1 := a.GetOutput()

	wf.Start()

	v := []flame.KeyValue[string, int]{
		{Key: "charles", Value: 3},
		{Key: "bob", Value: 2},
		{Key: "alice", Value: 1},
		{Key: "dan", Value: 4},
		{Key: "edward", Value: 5},
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
