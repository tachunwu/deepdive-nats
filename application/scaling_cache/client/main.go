package main

import (
	"fmt"
	"sync"
	"time"

	natsclient "github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
)

var wg sync.WaitGroup
var worker = 16

func main() {
	nc, err := natsclient.Connect("nats://localhost:4222")
	if err != nil {
		fmt.Println(err)
		return
	}
	start := time.Now()

	wg.Add(4)
	go OperationToPool(nc)
	go OperationToShard(nc, "shard.0")
	go OperationToShard(nc, "shard.1")
	go OperationToShard(nc, "shard.2")
	wg.Wait()
	fmt.Println("TPS for cache", 1200000/time.Since(start).Seconds())

}

func OperationToPool(nc *natsclient.Conn) {
	defer wg.Done()
	n := nuid.New()
	th := NewThrottle(worker)
	for i := 0; i < 100000; i++ {
		th.Do()
		go func(th *Throttle) {
			defer th.Done(nil)
			key := n.Next()
			// Set
			mSet := &natsclient.Msg{
				Header: natsclient.Header{},
				Data:   []byte("valueToPool"),
			}
			mSet.Header.Add("operation", "set")
			mSet.Header.Add("key", key)
			mSet.Header.Add("ttl", "0")
			mSet.Subject = "pool"
			nc.PublishMsg(mSet)

			// Get
			mGet := &natsclient.Msg{
				Header: natsclient.Header{},
			}
			mGet.Header.Add("operation", "get")
			mGet.Header.Add("key", key)
			mGet.Subject = "pool"
			_, err := nc.RequestMsg(mGet, time.Second)

			if err != nil {
				fmt.Println(err)
				return
			}

			// Delete
			mDelete := &natsclient.Msg{
				Header: natsclient.Header{},
			}
			mDelete.Header.Add("operation", "delete")
			mDelete.Header.Add("key", key)
			mDelete.Subject = "pool"
			_, err = nc.RequestMsg(mDelete, time.Second)

			if err != nil {
				fmt.Println(err)
				return
			}
		}(th)

	}

}

func OperationToShard(nc *natsclient.Conn, shard string) {
	defer wg.Done()
	n := nuid.New()
	key := n.Next()
	th := NewThrottle(worker)
	for i := 0; i < 100000; i++ {
		th.Do()
		go func(th *Throttle) {
			defer th.Done(nil)
			mSet := &natsclient.Msg{
				Header: natsclient.Header{},
				Data:   []byte("valueToShard"),
			}
			mSet.Header.Add("operation", "set")
			mSet.Header.Add("key", key)
			mSet.Header.Add("ttl", "1")
			mSet.Subject = shard
			nc.PublishMsg(mSet)

			mGet := &natsclient.Msg{
				Header: natsclient.Header{},
			}
			mGet.Header.Add("operation", "get")
			mGet.Header.Add("key", key)
			mGet.Subject = "pool"
			_, err := nc.RequestMsg(mGet, time.Second)

			if err != nil {
				fmt.Println(err)
				return
			}

			mDelete := &natsclient.Msg{
				Header: natsclient.Header{},
			}
			mDelete.Header.Add("operation", "delete")
			mDelete.Header.Add("key", n.Next())
			mDelete.Subject = "pool"
			_, err = nc.RequestMsg(mDelete, time.Second)

			if err != nil {
				fmt.Println(err)
				return
			}
		}(th)

	}
}

type Throttle struct {
	once      sync.Once
	wg        sync.WaitGroup
	ch        chan struct{}
	errCh     chan error
	finishErr error
}

// NewThrottle creates a new throttle with a max number of workers.
func NewThrottle(max int) *Throttle {
	return &Throttle{
		ch:    make(chan struct{}, max),
		errCh: make(chan error, max),
	}
}

// Do should be called by workers before they start working. It blocks if there
// are already maximum number of workers working. If it detects an error from
// previously Done workers, it would return it.
func (t *Throttle) Do() error {
	for {
		select {
		case t.ch <- struct{}{}:
			t.wg.Add(1)
			return nil
		case err := <-t.errCh:
			if err != nil {
				return err
			}
		}
	}
}

// Done should be called by workers when they finish working. They can also
// pass the error status of work done.
func (t *Throttle) Done(err error) {
	if err != nil {
		t.errCh <- err
	}
	select {
	case <-t.ch:
	default:
		panic("Throttle Do Done mismatch")
	}
	t.wg.Done()
}

// Finish waits until all workers have finished working. It would return any error passed by Done.
// If Finish is called multiple time, it will wait for workers to finish only once(first time).
// From next calls, it will return same error as found on first call.
func (t *Throttle) Finish() error {
	t.once.Do(func() {
		t.wg.Wait()
		close(t.ch)
		close(t.errCh)
		for err := range t.errCh {
			if err != nil {
				t.finishErr = err
				return
			}
		}
	})

	return t.finishErr
}
