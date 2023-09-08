package wpool

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

const (
	workerPoolWaitTimeout            = time.Second * 5
	defaultGroupsResponseChannelSize = 32
)

type Pool[Req any, Resp any] struct {
	handler      func(Req) Resp
	tasks        chan *task[Req, Resp]
	groupsPool   sync.Pool
	tasksPool    sync.Pool
	workersCount int64
	workersLimit int64
	groupID      int64
}

type Group[Req any, Resp any] struct {
	id              int64
	ctx             context.Context
	handler         func(t *task[Req, Resp])
	ch              chan Resp
	counter         int64
	acquireTaskFunc func() *task[Req, Resp]
}

type task[Req any, Resp any] struct {
	req Req
	ch  chan<- Resp
}

type Options struct {
	WorkersLimit int
}

func New[Req any, Resp any](handler func(Req) Resp, opts *Options) *Pool[Req, Resp] {
	wp := &Pool[Req, Resp]{
		handler: handler,
		tasks:   make(chan *task[Req, Resp]),
	}

	if opts != nil {
		wp.workersLimit = int64(opts.WorkersLimit)
	}

	return wp
}

func (w *Pool[Req, Resp]) AcquireGroup(ctx context.Context) *Group[Req, Resp] {
	g := w.groupsPool.Get()
	if g == nil {
		return &Group[Req, Resp]{
			id:              atomic.AddInt64(&w.groupID, 1),
			ctx:             ctx,
			handler:         w.task,
			ch:              make(chan Resp, defaultGroupsResponseChannelSize),
			acquireTaskFunc: w.acquireTask,
		}
	}
	gg := g.(*Group[Req, Resp])
	gg.ctx = ctx
	return gg
}

func (w *Pool[Req, Resp]) ReleaseGroup(g *Group[Req, Resp]) {
	// if group is busy, let GC collect it later
	if atomic.LoadInt64(&g.counter) == 0 {
		w.groupsPool.Put(g)
	}
}

func (g *Group[Req, Resp]) Wait(dest []Resp) []Resp {
	for {
		select {
		case <-g.ctx.Done():
			return dest
		case v := <-g.ch:
			dest = append(dest, v)
			if atomic.AddInt64(&g.counter, -1) == 0 {
				return dest
			}
		}
	}
}

func (g *Group[Req, Resp]) Go(req Req) {
	atomic.AddInt64(&g.counter, 1)
	t := g.acquireTaskFunc()
	t.ch = g.ch
	t.req = req
	g.handler(t)
}

func (w *Pool[Req, Resp]) task(t *task[Req, Resp]) {
	select {
	case w.tasks <- t:
	default:
		// if workers limit is not set, then create new worker
		if w.workersLimit <= 0 {
			go w.newWorker(t)
			return
		}
		if atomic.LoadInt64(&w.workersCount) < w.workersLimit {
			go w.newWorker(t)
			return
		}
		w.newWorker(t)
		return
	}
}

func (w *Pool[Req, Resp]) newWorker(t *task[Req, Resp]) {
	atomic.AddInt64(&w.workersCount, 1)
	defer atomic.AddInt64(&w.workersCount, -1)

	if t != nil {
		resp := w.handler(t.req)
		t.ch <- resp
		w.releaseTask(t)
	}

	timer := time.NewTimer(workerPoolWaitTimeout)
	defer timer.Stop()

	for {
		select {
		case t = <-w.tasks:
			resp := w.handler(t.req)
			t.ch <- resp
			w.releaseTask(t)
			timer.Reset(workerPoolWaitTimeout)
		case <-timer.C:
			return
		}
	}
}

func (w *Pool[Req, Resp]) acquireTask() *task[Req, Resp] {
	t := w.tasksPool.Get()
	if t == nil {
		return &task[Req, Resp]{}
	}
	return t.(*task[Req, Resp])
}

func (w *Pool[Req, Resp]) releaseTask(t *task[Req, Resp]) {
	w.tasksPool.Put(t)
}
