
# FLAME: FLow frAMEwork

A proof of concept flow processing library built on Go Generics, which were 
introduced in 1.18.


## Basic Map example
```
func Inc(a int) int {
	return a + 1
}

func main() {
	in := make(chan int, 10)

	wf := flame.NewWorkflow()
	inc := flame.AddSourceChan(wf, in)
	a := flame.AddMapper(wf, Inc)
	a.Connect(inc)
	b := flame.AddMapper(wf, Inc)
	b.Connect(a)
	out := b.GetOutput()

	wf.Start()

	v := []int{1, 2, 3, 4, 5}

	go func() {
		for _, n := range v {
			in <- n
		}
		close(in)
	}()

	i := 0
	for o := range out {
        fmt.Printf("out: %s\n", o)
    }
}
```