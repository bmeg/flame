package flame

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"log"
	"os"
	"path/filepath"

	"github.com/cockroachdb/pebble"
)

/**************************/
// KeyReducer
/**************************/

type byteAble interface {
	string
}

const (
	maxWriterBuffer = 3 << 30
)

type ReduceKeyNode[K byteAble, X, Y any] struct {
	Init    Y
	Input   chan KeyValue[K, X]
	Outputs []chan KeyValue[K, Y]
	Proc    func(K, X, Y) Y
}

func AddReduceKey[K byteAble, X, Y any](w *Workflow, f func(K, X, Y) Y, init Y) Node[KeyValue[K, X], KeyValue[K, Y]] {
	n := &ReduceKeyNode[K, X, Y]{Proc: f, Outputs: []chan KeyValue[K, Y]{}, Init: init}
	w.Nodes = append(w.Nodes, n)
	return n
}

func (n *ReduceKeyNode[K, X, Y]) Start(wf *Workflow) {
	wf.WaitGroup.Add(1)

	jsonChan := make(chan KeyValue[K, []byte], 10)
	go func() {
		dbDir, _ := wf.GetTmpDir()
		db, _ := pebble.Open(dbDir, &pebble.Options{})
		tfile := filepath.Join(dbDir, "dump.data")

		log.Printf("Reduce file: %s", tfile)
		dump, err := os.Create(tfile)
		if err != nil {
			log.Printf("%s", err)
		}

		if n.Input != nil {
			batch := db.NewBatch()
			curSize := int(0)
			for x := range n.Input {
				//marshal the data, and get the size
				data, _ := json.Marshal(x.Value)
				dSize := uint64(len(data))

				//get current location in data file
				stat, _ := dump.Stat()
				dPos := uint64(stat.Size())

				//encode postion and size
				bPos := make([]byte, 8)
				binary.BigEndian.PutUint64(bPos, dPos)
				bSize := make([]byte, 8)
				binary.BigEndian.PutUint64(bSize, dSize)
				//The db key is concats the user key followed by the position in the file
				//The means entries with the same user key will be stored in seperate
				//records
				key := bytes.Join([][]byte{[]byte(x.Key), bPos}, []byte{})
				batch.Set(key, bSize, nil)
				curSize += len(key) + len(bSize)
				if curSize > maxWriterBuffer {
					batch.Commit(nil)
					batch.Reset()
					curSize = 0
				}
				dump.Write(data)
				dump.Write([]byte("\n"))
			}
			batch.Commit(nil)
			batch.Close()
		}

		it := db.NewIter(&pebble.IterOptions{})
		for it.First(); it.Valid(); it.Next() {
			k := it.Key()
			dSize := binary.BigEndian.Uint64(it.Value())
			key := k[0 : len(k)-8]
			bPos := k[len(k)-8:]
			dPos := binary.BigEndian.Uint64(bPos)
			data := make([]byte, dSize)
			dump.ReadAt(data, int64(dPos))
			jsonChan <- KeyValue[K, []byte]{Key: K(key), Value: data}
		}
		dump.Close()
		db.Close()
		os.RemoveAll(dbDir)
		close(jsonChan)
	}()

	dataChan := make(chan KeyValue[K, X], 10)
	go func() {
		defer close(dataChan)
		for b := range jsonChan {
			var o X
			if err := json.Unmarshal(b.Value, &o); err == nil {
				dataChan <- KeyValue[K, X]{Key: b.Key, Value: o}
			}
		}
	}()

	go func() {
		first := true
		var key K
		var last Y
		for d := range dataChan {
			if d.Key != key {
				if !first {
					for i := range n.Outputs {
						n.Outputs[i] <- KeyValue[K, Y]{key, last}
					}
				} else {
					first = false
				}
				key = d.Key
				last = n.Proc(key, d.Value, n.Init)
			} else {
				last = n.Proc(key, d.Value, last)
			}
		}
		if !first {
			for i := range n.Outputs {
				n.Outputs[i] <- KeyValue[K, Y]{key, last}
			}
		}
		for i := range n.Outputs {
			close(n.Outputs[i])
		}
		wf.WaitGroup.Done()
	}()
}

func (n *ReduceKeyNode[K, X, Y]) GetOutput() chan KeyValue[K, Y] {
	m := make(chan KeyValue[K, Y])
	n.Outputs = append(n.Outputs, m)
	return m
}

func (n *ReduceKeyNode[K, X, Y]) Connect(e Emitter[KeyValue[K, X]]) {
	o := e.GetOutput()
	n.Input = o
}
