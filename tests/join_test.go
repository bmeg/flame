package tests

import (
  "testing"

	"github.com/bmeg/flame"
)

func KeyJoin(k string, x, y []int) int {
  s := 0
  for _, v := range x { s += v }
  for _, v := range y { s += v }
	return s
}

func TestJoinKey(t *testing.T) {
	left := make(chan flame.KeyValue[string, int], 10)
  right := make(chan flame.KeyValue[string, int], 10)

	wf := flame.NewWorkflow()
	wf.SetWorkDir("./")
	inL := flame.AddSourceChan(wf, left)
  inR := flame.AddSourceChan(wf, right)
	a := flame.AddJoinKey(wf, KeyJoin)

	a.ConnectLeft(inL)
  a.ConnectRight(inR)

	out1 := a.GetOutput()
	wf.Start()

	v1 := []flame.KeyValue[string, int]{
    {"b", 2},
		{"a", 1},
		{"c", 4},
	}
	go func() {
		for _, n := range v1 {
			left <- n
		}
		close(left)
	}()

  v2 := []flame.KeyValue[string, int]{
    {"c", 10},
    {"a", 3},
    {"b", 8},
  }
  go func() {
    for _, n := range v2 {
      right <- n
    }
    close(right)
  }()

	results := map[string]int{
		"a": 4,
		"b": 10,
		"c": 14,
	}

  count := 0
	for y := range out1 {
    count++
    //fmt.Printf("out: %#v\n", y)
		if y.Value != results[y.Key] {
			t.Errorf("Incorrect output")
		}
	}
  if count != 3 {
    t.Errorf("Incorrect output count")
  }

	wf.Wait()
}
