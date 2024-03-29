package wpool

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultWorkerTimeout             = time.Second * 5
	defaultGroupsResponseChannelSize = 32
)

// Pool is a worker pool
type Pool[Req any, Resp any] struct {
	handler                  func(Req) Resp
	tasks                    chan *task[Req, Resp]
	groupsPool               sync.Pool
	tasksPool                sync.Pool
	workersCount             int64
	workersLimitMax          int64
	workersLimitMin          int64
	stopWorkerTimeout        time.Duration
	groupResponseChannelSize int
}

// Group is a group of tasks
type Group[Req any, Resp any] struct {
	handler         func(t *task[Req, Resp])
	ch              chan Resp
	counter         int64
	acquireTaskFunc func() *task[Req, Resp]
}

type task[Req any, Resp any] struct {
	req Req
	ch  chan<- Resp
}

// Options is a pool options
type Options struct {
	// WorkersLimitMax is a maximum workers count, default 0 (unlimited)
	WorkersLimitMax int

	// WorkersLimitMin is a minimum workers count, default 0 (unlimited)
	WorkersLimitMin int

	// StopWorkerTimeout is a timeout for worker to stop, default 5 seconds
	StopWorkerTimeout time.Duration

	// GroupResponseChannelSize is the size of group response channel, default 32.
	// Sized channel is used to receive responses from workers while waiting group.Wait call.
	GroupResponseChannelSize int
}

// New creates new worker pool
func New[Req any, Resp any](handler func(Req) Resp, opts *Options) *Pool[Req, Resp] {
	wp := &Pool[Req, Resp]{
		handler:                  handler,
		tasks:                    make(chan *task[Req, Resp]),
		stopWorkerTimeout:        defaultWorkerTimeout,
		groupResponseChannelSize: defaultGroupsResponseChannelSize,
	}

	if opts != nil {
		if opts.WorkersLimitMax > 0 {
			wp.workersLimitMax = int64(opts.WorkersLimitMax)
		}
		if opts.StopWorkerTimeout > 0 {
			wp.stopWorkerTimeout = opts.StopWorkerTimeout
		}
		if opts.GroupResponseChannelSize > 0 {
			wp.groupResponseChannelSize = opts.GroupResponseChannelSize
		}
		if opts.WorkersLimitMin > 0 {
			wp.workersLimitMin = int64(opts.WorkersLimitMin)
			atomic.AddInt64(&wp.workersCount, int64(opts.WorkersLimitMin))
			for i := 0; i < opts.WorkersLimitMin; i++ {
				go wp.newWorker(nil)
			}
		}
	}

	return wp
}

// AcquireGroup acquires the new group.
// Use `group.Wait` to wait for all tasks in group to be done.
// You should call ReleaseGroup after `group.Wait` is done.
// You must not use the group after calling ReleaseGroup.
func (w *Pool[Req, Resp]) AcquireGroup() *Group[Req, Resp] {
	g := w.groupsPool.Get()
	if g == nil {
		return &Group[Req, Resp]{
			handler:         w.task,
			ch:              make(chan Resp, w.groupResponseChannelSize),
			acquireTaskFunc: w.acquireTask,
		}
	}
	gg := g.(*Group[Req, Resp])
	return gg
}

// ReleaseGroup releases group
// You must not use group after calling ReleaseGroup.
func (w *Pool[Req, Resp]) ReleaseGroup(g *Group[Req, Resp]) {
	// if the group is busy, let GC collect it later
	if atomic.LoadInt64(&g.counter) == 0 {
		w.groupsPool.Put(g)
	}
}

// WorkersCount returns current workers count
func (w *Pool[Req, Resp]) WorkersCount() int64 {
	return atomic.LoadInt64(&w.workersCount)
}

// Wait waits for all tasks in group to be done or context is done.
func (g *Group[Req, Resp]) Wait(ctx context.Context, dest []Resp) []Resp {
	if atomic.LoadInt64(&g.counter) == 0 {
		return dest
	}
	for {
		select {
		case <-ctx.Done():
			return dest
		case v := <-g.ch:
			dest = append(dest, v)
			if atomic.AddInt64(&g.counter, -1) == 0 {
				return dest
			}
		}
	}
}

// Go runs the task in the group (unblocking)
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
		count := atomic.AddInt64(&w.workersCount, 1)

		// if the worker max limit is not set, or we did not exceed it, then create a new worker
		if w.workersLimitMax <= 0 || count <= w.workersLimitMax {
			go w.newWorker(t)
			return
		}

		// if the worker max limit is set, and we exceeded it, then wait for free worker
		atomic.AddInt64(&w.workersCount, -1)
		w.tasks <- t
		return
	}
}

func (w *Pool[Req, Resp]) newWorker(t *task[Req, Resp]) {
	defer atomic.AddInt64(&w.workersCount, -1)

	if t != nil {
		resp := w.handler(t.req)
		t.ch <- resp
		w.releaseTask(t)
	}

	timer := time.NewTimer(w.stopWorkerTimeout)
	defer timer.Stop()

	for {
		select {
		case t = <-w.tasks:
			resp := w.handler(t.req)
			t.ch <- resp
			w.releaseTask(t)
			timer.Reset(w.stopWorkerTimeout)
		case <-timer.C:
			if atomic.LoadInt64(&w.workersCount) > w.workersLimitMin {
				return
			}
			timer.Reset(w.stopWorkerTimeout)
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
