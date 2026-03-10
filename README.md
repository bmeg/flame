
# FLAME: FLow frAMEwork

A flow processing library built on Go Generics


## Map example
```go
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

## Reduce example
```go
func Sum(a, b int) int { return a + b }

func main() {
    in := make(chan int, 5)
    wf := flame.NewWorkflow()
    src := flame.AddSourceChan(wf, in)
    // Reduce starts with initial value 0 and applies Sum to each element.
    red := flame.AddReducer[int, int](wf, Sum, 0)
    red.Connect(src)
    out := red.GetOutput()
    wf.Start()
    go func(){
        for _, n := range []int{1,2,3,4,5} { in <- n }
        close(in)
    }()
    fmt.Printf("sum: %d\n", <-out)
}
```

## FlatMap example
```go
func Split(s string) []string { return strings.Split(s, "") }

func main() {
    in := make(chan string, 1)
    wf := flame.NewWorkflow()
    src := flame.AddSourceChan(wf, in)
    fm := flame.AddFlatMapper(wf, Split)
    fm.Connect(src)
    out := fm.GetOutput()
    wf.Start()
    in <- "hello"
    close(in)
    for v := range out { fmt.Println(v) }
}
```

## Join (zip) example
```go
func main() {
    // Create source channels
    aChan := make(chan int)
    bChan := make(chan string)
    wf := flame.NewWorkflow()
    // Add source nodes
    a := flame.AddSourceChan(wf, aChan)
    b := flame.AddSourceChan(wf, bChan)
    // Join node that zips int and string into a struct
    j := flame.AddJoin[int, string, struct{I int; S string}](wf, func(l <-chan int, r <-chan string, out chan struct{I int; S string}) {
        for {
            lv, lok := <-l
            rv, rok := <-r
            if !lok || !rok { break }
            out <- struct{I int; S string}{lv, rv}
        }
    })
    j.ConnectLeft(a)
    j.ConnectRight(b)
    out := j.GetOutput()
    wf.Start()
    // Feed data
    go func(){
        for _, v := range []int{1,2,3} { aChan <- v }
        close(aChan)
    }()
    go func(){
        for _, v := range []string{"a","b","c"} { bChan <- v }
        close(bChan)
    }()
    for v := range out { fmt.Printf("%v\n", v) }
}
```

## Join Example
```go
func main() {
    a := flame.AddSourceSlice([]int{1,2,3})
    b := flame.AddSourceSlice([]string{"a","b","c"})
    wf := flame.NewWorkflow()
    j := flame.AddJoin[int, string, struct{I int; S string}](wf, func(l <-chan int, r <-chan string, out chan struct{I int; S string}) {
        for {
            lv, lok := <-l
            rv, rok := <-r
            if !lok || !rok { break }
            out <- struct{I int; S string}{lv, rv}
        }
    })
    j.ConnectLeft(a)
    j.ConnectRight(b)
    out := j.GetOutput()
    wf.Start()
    for v := range out { fmt.Printf("%v\n", v) }
}
```

## Source → sink example
```go
func main() {
    wf := flame.NewWorkflow()
    src := flame.AddSourceSlice([]int{1,2,3,4})
    sink := flame.AddSink[int](wf, func(v int){ fmt.Println("got", v) })
    sink.Connect(src)
    wf.Start()
    wf.Wait()
}
```

