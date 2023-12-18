package wpool

import (
	"context"
	"testing"
	"time"
)

func TestSimple(t *testing.T) {
	handler := func(r int) int {
		return r * 2
	}

	wp := New[int, int](handler, nil)

	if wp.WorkersCount() != 0 {
		t.Fatal("workers count must be 0")
	}

	g := wp.AcquireGroup()

	g.Go(1)
	g.Go(2)
	g.Go(3)
	g.Go(4)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()

	start := time.Now()
	resp := g.Wait(ctx, nil)
	end := time.Since(start)

	if end.Milliseconds() > 1 {
		t.Fatal("too slow")
	}

	expect := map[int]struct{}{2: {}, 4: {}, 6: {}, 8: {}}

	for _, r := range resp {
		_, ok := expect[r]
		if !ok {
			t.Fatal("unexpected response")
		}
		delete(expect, r)
	}

	if len(expect) > 0 {
		t.Fatal("not all responses received")
	}

	if wp.WorkersCount() != 4 {
		t.Fatal("workers count must be 4")
	}
}

func TestOneWorker(t *testing.T) {
	handler := func(r int) int {
		return r * 2
	}

	wp := New[int, int](handler, nil)

	if wp.WorkersCount() != 0 {
		t.Fatal("workers count must be 0")
	}

	g := wp.AcquireGroup()

	// pauses for reuse worker

	g.Go(1)
	time.Sleep(time.Millisecond * 50)
	g.Go(2)
	time.Sleep(time.Millisecond * 50)
	g.Go(3)
	time.Sleep(time.Millisecond * 50)
	g.Go(4)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()

	start := time.Now()
	resp := g.Wait(ctx, nil)
	end := time.Since(start)

	if end.Milliseconds() > 1 {
		t.Fatal("too slow")
	}

	expect := map[int]struct{}{2: {}, 4: {}, 6: {}, 8: {}}

	for _, r := range resp {
		_, ok := expect[r]
		if !ok {
			t.Fatal("unexpected response")
		}
		delete(expect, r)
	}

	if len(expect) > 0 {
		t.Fatal("not all responses received")
	}

	if wp.WorkersCount() != 1 {
		t.Fatal("workers count must be 1")
	}
}

func TestMinWorkers(t *testing.T) {
	handler := func(r int) int {
		return r * 2
	}

	wp := New[int, int](handler, &Options{
		WorkersLimitMin: 10,
	})

	// pause for workers creation
	time.Sleep(time.Millisecond * 50)

	if count := wp.WorkersCount(); count != 10 {
		t.Fatalf("workers count must be 10, got %d", count)
	}

	g := wp.AcquireGroup()

	g.Go(1)
	time.Sleep(time.Millisecond * 50)
	g.Go(2)
	time.Sleep(time.Millisecond * 50)
	g.Go(3)
	time.Sleep(time.Millisecond * 50)
	g.Go(4)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()

	start := time.Now()
	resp := g.Wait(ctx, nil)
	end := time.Since(start)

	if end.Milliseconds() > 1 {
		t.Fatal("too slow")
	}

	expect := map[int]struct{}{2: {}, 4: {}, 6: {}, 8: {}}

	for _, r := range resp {
		_, ok := expect[r]
		if !ok {
			t.Fatal("unexpected response")
		}
		delete(expect, r)
	}

	if len(expect) > 0 {
		t.Fatal("not all responses received")
	}

	if wp.WorkersCount() != 10 {
		t.Fatal("workers count must be 10")
	}
}

func TestMaxWorkers(t *testing.T) {
	handler := func(r int) int {
		return r * 2
	}

	wp := New[int, int](handler, &Options{
		WorkersLimitMax: 2,
	})

	if count := wp.WorkersCount(); count != 0 {
		t.Fatalf("workers count must be 0, got %d", count)
	}

	g := wp.AcquireGroup()

	g.Go(1)
	g.Go(2)
	g.Go(3)
	g.Go(4)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()

	start := time.Now()
	resp := g.Wait(ctx, nil)
	end := time.Since(start)

	if end.Milliseconds() > 1 {
		t.Fatal("too slow")
	}

	expect := map[int]struct{}{2: {}, 4: {}, 6: {}, 8: {}}

	for _, r := range resp {
		_, ok := expect[r]
		if !ok {
			t.Fatal("unexpected response")
		}
		delete(expect, r)
	}

	if len(expect) > 0 {
		t.Fatal("not all responses received")
	}

	if count := wp.WorkersCount(); count != 2 {
		t.Fatalf("workers count must be 2, got %d", count)
	}
}

func TestWaitDoneByCtx(t *testing.T) {
	handler := func(r int) int {
		if r == 2 {
			time.Sleep(time.Second)
		}
		return r * 2
	}

	wp := New[int, int](handler, &Options{})

	g := wp.AcquireGroup()

	g.Go(1)
	g.Go(2)
	g.Go(3)
	g.Go(4)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()

	start := time.Now()
	resp := g.Wait(ctx, nil)
	end := time.Since(start)

	if end.Milliseconds() > 110 {
		t.Fatal("too slow")
	}

	expect := map[int]struct{}{2: {}, 6: {}, 8: {}}

	for _, r := range resp {
		_, ok := expect[r]
		if !ok {
			t.Fatal("unexpected response")
		}
		delete(expect, r)
	}

	if len(expect) > 0 {
		t.Fatal("not all responses received")
	}
}

func TestStopWorkersByTimeout(t *testing.T) {
	handler := func(r int) int {
		return r * 2
	}

	wp := New[int, int](handler, &Options{
		StopWorkerTimeout: time.Millisecond * 100,
	})

	if count := wp.WorkersCount(); count != 0 {
		t.Fatalf("workers count must be 0, got %d", count)
	}

	g := wp.AcquireGroup()

	g.Go(1)
	g.Go(2)
	g.Go(3)
	g.Go(4)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()

	start := time.Now()
	resp := g.Wait(ctx, nil)
	end := time.Since(start)

	if end.Milliseconds() > 110 {
		t.Fatal("too slow")
	}

	expect := map[int]struct{}{2: {}, 4: {}, 6: {}, 8: {}}

	for _, r := range resp {
		_, ok := expect[r]
		if !ok {
			t.Fatal("unexpected response")
		}
		delete(expect, r)
	}

	if len(expect) > 0 {
		t.Fatal("not all responses received")
	}

	if count := wp.WorkersCount(); count != 4 {
		t.Fatalf("workers count must be 4, got %d", count)
	}

	time.Sleep(time.Second)

	if count := wp.WorkersCount(); count != 0 {
		t.Fatalf("workers count must be 0, got %d", count)
	}
}
