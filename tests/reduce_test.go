package tests

import (
	"fmt"
	"github.com/bmeg/flame"
	"testing"
)

func Sum(x, y int) int {
	return x + y
}

func TestReduce(t *testing.T) {
	in := make(chan int, 10)

	wf := flame.NewWorkflow()
	inc := flame.AddSourceChan(wf, in)
	a := flame.AddReducer(wf, Sum, 0)
	a.Connect(inc)

	out1 := a.GetOutput()
	wf.Start()

	v := []int{1, 2, 3, 4, 5}
	go func() {
		for _, n := range v {
			in <- n
		}
		close(in)
	}()

	total := 0
	for _, j := range v {
		total += j
	}
	for y := range out1 {
		if y != total {
			t.Errorf("Incorrect Values %d != %d", y, total)
		}
		fmt.Printf("sum: %d\n", y)
	}

}
