# Go Worker Pool with Groups

Simple example

```go
package main

import (
	"context"
	"fmt"
	"time"
	
	"github.com/negasus/wpool"
)

type request struct {}

type response struct {}

func main() {
	wp := wpool.New[*request, *response](handler, nil)

	g := wp.AcquireGroup()
	defer wp.ReleaseGroup(g)

	g.Go(&request{})
	g.Go(&request{})
	g.Go(&request{})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	resp := g.Wait(ctx, nil)

	for _, r := range resp {
		fmt.Printf("%#v\n", r)
	}
}

func handler(req *request) *response {
	resp := &response{}
	
	// do something
	
	return resp
}

```

## Changelog

### v0.1.0

- initial release
