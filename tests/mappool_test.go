package tests

import (
	"testing"

	"github.com/bmeg/flame"
)

func TestMapPool(t *testing.T) {
	in := make(chan int, 10)

	wf := flame.NewWorkflow()
	inc := flame.AddSourceChan(wf, in)
	a := flame.AddMapperPool(wf, Inc, 10)
	a.Connect(inc)
	b := flame.AddMapperPool(wf, Inc, 3)
	b.Connect(a)
	out := b.GetOutput()

	wf.Start()

	v := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}

	go func() {
		for _, n := range v {
			in <- n
		}
		close(in)
	}()

	i := 0
	last := 2
	for o := range out {
		if v[i]+2 != o {
			t.Errorf("Incorrect Values %d != %d", o, v[i])
		}
		if o != last+1 {
			t.Errorf("Incorrect order %d -> %d", last, o)
		}
		last = o
		i += 1
	}

	if i != len(v) {
		t.Errorf("Incorrect count received: %d != %d", i, len(v))
	}
}
