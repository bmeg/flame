package tests

import (
	"fmt"
	"strings"
	"testing"

	"github.com/bmeg/flame"
)

func Sep(x string) []string {
	return strings.Split(x, "")
}

func TestFlatMap(t *testing.T) {
	in := make(chan string, 10)

	wf := flame.NewWorkflow()
	inc := flame.AddSourceChan(wf, in)
	a := flame.AddFlatMapper(wf, Sep)
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
		//fmt.Printf("%#v\n", y)
		if len(y) != 1 {
			t.Errorf("Incorrect length output")
		}
		count += 1
	}
	if count != len(v[0])+len(v[1]) {
		t.Errorf("Incorrect output count")
	}
}

func TestFlatMapPool(t *testing.T) {
	in := make(chan string, 10)

	wf := flame.NewWorkflow()
	inc := flame.AddSourceChan(wf, in)
	a := flame.AddFlatMapperPool(wf, Sep, 4)
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
		fmt.Printf("flat out: %#v\n", y)
		if len(y) != 1 {
			t.Errorf("Incorrect length output")
		}
		count += 1
	}
	if count != len(v[0])+len(v[1]) {
		t.Errorf("Incorrect output count %d != %d", count, len(v[0])+len(v[1]))
	}
}
