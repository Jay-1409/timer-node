package timer

import (
	"container/heap"
	"log"
	"sync"
	"time"
)

type TimerTaskHeap []*TimerTask

type TimerHeap struct {
	ID           int
	Tasks        TimerTaskHeap
	MaxTaskSize  int
	mu           sync.Mutex
	EventHandler *TimerEventHandler
	taskAdded    chan struct{} // Channel to signal new task arrivals
}

func NewTimerHeap(id int, queueSize int, workerCount int) *TimerHeap {
	t := &TimerHeap{
		ID:          id,
		Tasks:       make(TimerTaskHeap, 0, queueSize),
		MaxTaskSize: queueSize,
		taskAdded:   make(chan struct{}, 1),
	}

	heap.Init(&t.Tasks)
	t.EventHandler = NewTimerEventHandler(id, queueSize, workerCount)
	t.EventHandler.Handler()
	return t
}

func (h TimerTaskHeap) Len() int {
	return len(h)
}

func (h TimerTaskHeap) Less(i, j int) bool {
	return h[i].FireAt.Before(h[j].FireAt)
}

func (h TimerTaskHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *TimerTaskHeap) Push(x any) {
	*h = append(*h, x.(*TimerTask))
}

func (h *TimerTaskHeap) Pop() any {
	old := *h
	n := len(old)

	task := old[n-1]
	*h = old[:n-1]

	return task
}

func (h TimerTaskHeap) Peek() *TimerTask {
	if len(h) == 0 {
		return nil
	}
	return h[0]
}

func (t *TimerHeap) AddTask(task *TimerTask) bool {
	t.mu.Lock()
	if t.Tasks.Len() >= t.MaxTaskSize {
		t.mu.Unlock()
		return false
	}

	heap.Push(&t.Tasks, task)
	t.mu.Unlock()

	// Notify the runner loop that a new task was added
	select {
	case t.taskAdded <- struct{}{}:
	default:
	}

	return true
}

func (t *TimerHeap) PeekTask() *TimerTask {
	return t.Tasks.Peek()
}

func (t *TimerHeap) PopTask() *TimerTask {
	if t.Tasks.Len() == 0 {
		return nil
	}

	return heap.Pop(&t.Tasks).(*TimerTask)
}

/**
	Wait until the top-most task reaches its completion state.
	Uses channels to sleep efficiently and wake up early if a new task arrives.
*/
func (t *TimerHeap) Run() {
	for {
		t.mu.Lock()
		next := t.PeekTask()
		t.mu.Unlock()

		if next == nil {
			// No tasks: sleep efficiently until a new task is added
			<-t.taskAdded
			continue
		}

		duration := time.Until(next.FireAt)
		if duration <= 0 {
			t.FireExpired()
			continue
		}

		// Sleep efficiently but wake up early if a new task arrives
		timer := time.NewTimer(duration)
		select {
		case <-timer.C:
			// Task is ready to fire
			t.FireExpired()
		case <-t.taskAdded:
			// A new task was added; stop the timer and re-evaluate the top task
			timer.Stop()
		}
	}
}

/**
	While we start firing one task it is possible that there are multiple timers that exist at small timer intervals
	Lets call this as TASK FLOODING. The synchronous nature of this function allows us to fire all of such timers before returning to the heap.

	The reason for taking locks on popping one task is to avoid the blocking of adding new tasks to the heap during a TASK FLOOD.
*/
func (t *TimerHeap) FireExpired() {
	now := time.Now()
	for {
		t.mu.Lock()
		top := t.PeekTask()
		if top == nil || top.FireAt.After(now) {
			t.mu.Unlock()
			return
		}
		expired := t.PopTask()
		t.mu.Unlock()
		log.Printf("fired task %s from heap: %d", expired.ID, t.ID)
		if t.EventHandler != nil {
			t.EventHandler.Dispatch(expired)
		}
	}
}
