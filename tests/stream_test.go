package tests

import (
	"fmt"
	"strings"
	"testing"

	"github.com/bmeg/flame"
)

func SepStream(in chan string, out chan []string) {
	for i := range in {
		out <- strings.Split(i, "")
	}
}

func TestStream(t *testing.T) {
	in := make(chan string, 10)

	wf := flame.NewWorkflow()
	inc := flame.AddSourceChan(wf, in)
	a := flame.AddStreamer(wf, SepStream)
	a.Connect(inc)

	out1 := a.GetOutput()
	wf.Start()

	v := []string{"hello", "world"}
	go func() {
		for _, n := range v {
			in <- n
		}
		close(in)
	}()

	count := 0
	for y := range out1 {
		fmt.Printf("%#v\n", y)
		if len(strings.Join(y, "")) != 5 {
			t.Errorf("Incorrect length output")
		}
		count += 1
	}
	if count != 2 {
		t.Errorf("Incorrect output count")
	}
}
