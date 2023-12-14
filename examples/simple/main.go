package main

import (
	"context"
	"fmt"
	"time"

	"github.com/negasus/wpool"
)

type request struct {
	id int
}

type response struct {
	reqId int
	value int
}

func main() {
	wp := wpool.New[*request, *response](handler, nil)

	g := wp.AcquireGroup()
	defer wp.ReleaseGroup(g)

	g.Go(&request{id: 1})
	g.Go(&request{id: 2})
	g.Go(&request{id: 3})
	g.Go(&request{id: 4})
	g.Go(&request{id: 5})

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*500)
	defer cancel()

	resp := g.Wait(ctx, nil)

	for _, r := range resp {
		fmt.Printf("%#v\n", r)
	}
}

func handler(req *request) *response {
	resp := &response{}

	if req.id == 4 {
		time.Sleep(time.Second)
	}

	resp.reqId = req.id
	resp.value = req.id * 2

	return resp
}
