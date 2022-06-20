package tests

import (
	"fmt"
	"testing"

	"github.com/bmeg/flame"
)

func keyAccumulate(key int, names []string) []string {
	return names
}

func TestAccumulate(t *testing.T) {
	in := make(chan flame.KeyValue[int, string], 10)

	wf := flame.NewWorkflow()
	inc := flame.AddSourceChan(wf, in)
	a := flame.AddAccumulate(wf, keyAccumulate)
	a.Connect(inc)

	out1 := a.GetOutput()
	wf.Start()

	v := []flame.KeyValue[int, string]{
		{1, "charles"},
		{1, "bob"},
		{2, "alice"},
		{2, "dan"},
		{3, "edward"},
		{3, "frank"},
	}

	go func() {
		for _, n := range v {
			in <- n
		}
		close(in)
	}()

	for y := range out1 {
		if len(y.Value) != 2 {
			t.Errorf("Incorrect accumulate count: %d", len(y.Value))
		}
		fmt.Printf("%d %s\n", y.Key, y.Value)
	}

}
