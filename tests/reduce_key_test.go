package tests

import (
	"testing"

	"github.com/bmeg/flame"
)

func KeySum(k string, x, y int) int {
	return x + y
}

func TestReduceKey(t *testing.T) {
	in := make(chan flame.KeyValue[string, int], 10)

	wf := flame.NewWorkflow()
	wf.SetWorkDir("./")
	inc := flame.AddSourceChan(wf, in)
	a := flame.AddReduceKey(wf, KeySum, 0)
	a.Connect(inc)

	out1 := a.GetOutput()
	wf.Start()

	v := []flame.KeyValue[string, int]{
		{Key: "a", Value: 1},
		{Key: "a", Value: 1},
		{Key: "a", Value: 1},
		{Key: "b", Value: 2},
		{Key: "b", Value: 2},
		{Key: "b", Value: 2},
		{Key: "c", Value: 4},
		{Key: "c", Value: 5},
		{Key: "c", Value: 6},
	}
	go func() {
		for _, n := range v {
			in <- n
		}
		close(in)
	}()

	results := map[string]int{
		"a": 3,
		"b": 6,
		"c": 15,
	}

	for y := range out1 {
		if y.Value != results[y.Key] {
			t.Errorf("Incorrect output")
		}
	}

	wf.Wait()
}
