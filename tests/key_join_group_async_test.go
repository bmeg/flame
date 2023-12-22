package tests

import (
	"fmt"
	"testing"

	"github.com/bmeg/flame"
)

func KeyJoinGroup(k string, x []int) map[string]any {
	out := map[string]any{}

	return out
}

func TestJoinGroupKeyAsync(t *testing.T) {

	inputs := make(chan flame.KeyValue[string, int], 10)

	wf := flame.NewWorkflow()
	wf.SetWorkDir("./")

	in := flame.AddSourceChan(wf, inputs)

	steps := []flame.Emitter[flame.KeyValue[string, int]]{}
	for i := 0; i < 5; i++ {
		inc := i
		m := flame.AddMapper(wf, func(x flame.KeyValue[string, int]) flame.KeyValue[string, int] {
			x.Value += inc
			return x
		})
		m.Connect(in)
		steps = append(steps, m)
	}

	as := flame.AddKeyJoinGroupAsync(wf, func(key string, x []int) []int {
		return x
	})

	for _, i := range steps {
		as.Connect(i)
	}

	fmt.Printf("Starting Workflow\n")
	wf.Start()

	v1 := []flame.KeyValue[string, int]{
		{Key: "a", Value: 1},
		{Key: "b", Value: 2},
		{Key: "c", Value: 3},
	}
	go func() {
		for _, n := range v1 {
			inputs <- n
		}
		close(inputs)
	}()

	/*
		results := map[string]int{
			"a": 4,
			"b": 10,
			"c": 14,
		}
	*/

	out := as.GetOutput()

	for i := range out {
		fmt.Printf("%#v\n", i)
	}

	wf.Wait()
}
