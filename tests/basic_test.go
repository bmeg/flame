package tests

import (
  "sync"
  "testing"
	"github.com/bmeg/flame"
)

func Inc(a int) int {
	return a + 1
}

type Multiply struct {
	val int
}

func (m *Multiply) Run(x int) int {
	return x * m.val
}

func TestSingle(t *testing.T) {
	in := make(chan int, 10)

	wf := flame.NewWorkflow()
	a := flame.AddMapper(wf, Inc)
	a.AddInput(in)
	b := flame.AddMapper(wf, Inc)
	b.Connect(a)
	out := b.GetOutput()
	wf.Init()

	v := []int{1, 2, 3, 4, 5}

	go func() {
		for _, n := range v {
			in <- n
		}
		close(in)
	}()

	i := 0
	for o := range out {
		if v[i]+2 != o {
			t.Errorf("Incorrect Values %d != %d", o, v[i])
		}
		i += 1
	}

	if i != len(v) {
		t.Errorf("Incorrect count received: %d != %d", i, len(v))
	}
}

func TestSplit(t *testing.T) {
	in := make(chan int, 10)

	wf := flame.NewWorkflow()
	a := flame.AddMapper(wf, Inc)
	a.AddInput(in)
	b := flame.AddMapper(wf, Inc)
	b.Connect(a)
	c := flame.AddMapper(wf, (&Multiply{6}).Run)
	c.Connect(a)

	out1 := b.GetOutput()
	out2 := c.GetOutput()
	wf.Init()

	v := []int{1, 2, 3, 4, 5}

	go func() {
		for _, n := range v {
			in <- n
		}
		close(in)
	}()

	wg := &sync.WaitGroup{}

	wg.Add(2)
	go func() {
		i := 0
		for o := range out1 {
			if v[i]+2 != o {
				t.Errorf("Incorrect Values %d != %d", o, v[i])
			}
			i += 1
		}
		if i != len(v) {
			t.Errorf("Incorrect count received: %d != %d", i, len(v))
		}
		wg.Done()
	}()
	go func() {
		i := 0
		for o := range out2 {
			if (v[i]+1)*6 != o {
				t.Errorf("Incorrect Values %d != %d", o, (v[i]+1)*6)
			}
			i += 1
		}
		if i != len(v) {
			t.Errorf("Incorrect count received: %d != %d", i, len(v))
		}
		wg.Done()
	}()

	wg.Wait()
}
