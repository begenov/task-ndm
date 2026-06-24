package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type waiter struct {
	ch chan string
}

type queue struct {
	msgs    []string
	waiters []*waiter
}

type broker struct {
	mu     sync.Mutex
	queues map[string]*queue
}

func (b *broker) q(name string) *queue {
	q, ok := b.queues[name]
	if !ok {
		q = &queue{}
		b.queues[name] = q
	}
	return q
}

func removeWaiter(q *queue, w *waiter) {
	for i, x := range q.waiters {
		if x == w {
			q.waiters = append(q.waiters[:i], q.waiters[i+1:]...)
			return
		}
	}
}

func (b *broker) put(name, msg string) {
	b.mu.Lock()
	q := b.q(name)
	if len(q.waiters) > 0 {
		w := q.waiters[0]
		q.waiters = q.waiters[1:]
		b.mu.Unlock()
		w.ch <- msg
		return
	}
	q.msgs = append(q.msgs, msg)
	b.mu.Unlock()
}

func (b *broker) get(name string, timeout time.Duration, ctx context.Context) (string, bool) {
	b.mu.Lock()
	q := b.q(name)
	if len(q.msgs) > 0 {
		msg := q.msgs[0]
		q.msgs = q.msgs[1:]
		b.mu.Unlock()
		return msg, true
	}
	if timeout == 0 {
		b.mu.Unlock()
		return "", false
	}
	w := &waiter{ch: make(chan string, 1)}
	q.waiters = append(q.waiters, w)
	b.mu.Unlock()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case msg := <-w.ch:
		return msg, true
	case <-timer.C:
		b.mu.Lock()
		removeWaiter(q, w)
		b.mu.Unlock()
	case <-ctx.Done():
		b.mu.Lock()
		removeWaiter(q, w)
		b.mu.Unlock()
	}
	select {
	case msg := <-w.ch:
		return msg, true
	default:
		return "", false
	}
}

func (b *broker) handle(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/")
	switch r.Method {
	case http.MethodPut:
		if r.URL.Query().Get("v") == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		b.put(name, r.URL.Query().Get("v"))
		w.WriteHeader(http.StatusOK)
	case http.MethodGet:
		var timeout time.Duration
		if s := r.URL.Query().Get("timeout"); s != "" {
			n, err := strconv.Atoi(s)
			if err != nil || n < 0 {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			timeout = time.Duration(n) * time.Second
		}
		msg, ok := b.get(name, timeout, r.Context())
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(msg))
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func main() {
	if len(os.Args) != 2 {
		fmt.Println(len(os.Args))

		os.Exit(1)
	}
	b := &broker{queues: make(map[string]*queue)}
	http.HandleFunc("/", b.handle)
	if err := http.ListenAndServe(":"+os.Args[1], nil); err != nil {
		os.Exit(1)
	}
}
