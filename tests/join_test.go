package tests

import (
	"fmt"
	"testing"

	"github.com/bmeg/flame"
)

func zip(a chan string, b chan string, out chan []string) {
	okA := true
	okB := true
	for okA && okB {
		stringA := ""
		stringB := ""
		stringA, okA = <-a
		stringB, okB = <-b
		if okA && okB {
			out <- []string{stringA, stringB}
		}
	}
}

func TestJoinZip(t *testing.T) {
	left := make(chan string, 10)
	right := make(chan string, 10)

	leftIn := []string{"Hello", "Alice", "Town"}
	rightIn := []string{"World", "Bob", "City"}

	go func() {
		for _, i := range leftIn {
			left <- i
		}
		close(left)
	}()

	go func() {
		for _, i := range rightIn {
			right <- i
		}
		close(right)
	}()

	wf := flame.NewWorkflow()
	wf.SetWorkDir("./")
	inL := flame.AddSourceChan(wf, left)
	inR := flame.AddSourceChan(wf, right)
	a := flame.AddJoin(wf, zip)

	a.ConnectLeft(inL)
	a.ConnectRight(inR)

	out1 := a.GetOutput()
	wf.Start()

	count := 0
	for y := range out1 {
		count++
		fmt.Printf("out: %#v\n", y)
		if len(y) != 2 {
			t.Errorf("Not zipped")
		}
	}
	if count != 3 {
		t.Errorf("Incorrect output count")
	}

	wf.Wait()

}
