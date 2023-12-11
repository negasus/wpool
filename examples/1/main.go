package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/negasus/wpool"
)

type request struct {
	id int
}

type response struct {
	value int
	sleep int
}

func main() {
	wp := wpool.New[*request, *response](handler, nil)

	wg := sync.WaitGroup{}
	wg.Add(3)

	go runGroup(1, wp, &wg)
	go runGroup(2, wp, &wg)
	go runGroup(3, wp, &wg)

	wg.Wait()

	fmt.Printf("done\n")
}

func runGroup(num int, w *wpool.Pool[*request, *response], wg *sync.WaitGroup) {
	defer wg.Done()

	g := w.AcquireGroup()
	defer w.ReleaseGroup(g)

	start := time.Now()

	for i := 0; i < 5; i++ {
		req := &request{}
		req.id = num*100 + i + 1
		g.Go(req)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	var resp []*response
	resp = g.Wait(ctx, resp)

	end := time.Since(start)

	for idx, r := range resp {
		fmt.Printf("(%.3f sec) Group %d result %d: %+v\n", end.Seconds(), num, idx, r)
	}
	fmt.Printf("\n")
}

func handler(r *request) *response {
	resp := &response{
		value: r.id,
	}

	var sleep int

	switch r.id {
	case 102:
		sleep = 1050
	case 203:
		sleep = 800
	case 301:
		sleep = 50
	}

	resp.sleep = sleep

	if sleep > 0 {
		time.Sleep(time.Millisecond * time.Duration(sleep))
	}

	return resp
}
