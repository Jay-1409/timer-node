package timer

import (
	"container/heap"
	"fmt"
	"io"
	"log"
	"testing"
	"time"
)

func init() {
	// Silence verbose log outputs during test / benchmark runs
	log.SetOutput(io.Discard)
}

func TestTimerHeap_PushPopOrder(t *testing.T) {
	h := make(TimerTaskHeap, 0)
	heap.Init(&h)

	now := time.Now()
	tasks := []*TimerTask{
		{ID: "task-3", FireAt: now.Add(3 * time.Second)},
		{ID: "task-1", FireAt: now.Add(1 * time.Second)},
		{ID: "task-4", FireAt: now.Add(4 * time.Second)},
		{ID: "task-2", FireAt: now.Add(2 * time.Second)},
	}

	for _, task := range tasks {
		heap.Push(&h, task)
	}

	expectedOrder := []string{"task-1", "task-2", "task-3", "task-4"}
	for _, expectedID := range expectedOrder {
		if h.Len() == 0 {
			t.Fatalf("expected more tasks, heap is empty")
		}
		popped := heap.Pop(&h).(*TimerTask)
		if popped.ID != expectedID {
			t.Errorf("expected task ID %s, got %s", expectedID, popped.ID)
		}
	}
}

func TestTimerHeap_CapacityLimit(t *testing.T) {
	th := NewTimerHeap(0, 5, 1)
	now := time.Now()

	for i := 0; i < 5; i++ {
		ok := th.AddTask(&TimerTask{
			ID:     fmt.Sprintf("task-%d", i),
			FireAt: now.Add(time.Duration(i+1) * time.Minute),
		})
		if !ok {
			t.Fatalf("expected AddTask to succeed for item %d", i)
		}
	}

	// 6th task should fail
	ok := th.AddTask(&TimerTask{
		ID:     "overflow",
		FireAt: now.Add(10 * time.Minute),
	})
	if ok {
		t.Fatalf("expected AddTask to return false when capacity reached")
	}
}

// Benchmark raw TimerTaskHeap Push and Pop operations
func BenchmarkTimerTaskHeap_PushPop(b *testing.B) {
	now := time.Now()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h := make(TimerTaskHeap, 0, 1000)
		heap.Init(&h)
		for j := 0; j < 1000; j++ {
			heap.Push(&h, &TimerTask{
				ID:     "task",
				FireAt: now.Add(time.Duration(j%100) * time.Millisecond),
			})
		}
		for h.Len() > 0 {
			heap.Pop(&h)
		}
	}
}

// Benchmark AddTask on a single TimerHeap with 1 goroutine
func BenchmarkTimerHeap_AddTask_SingleGoroutine(b *testing.B) {
	th := NewTimerHeap(0, b.N+10, 0)
	now := time.Now()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		th.AddTask(&TimerTask{
			ID:     "task",
			FireAt: now.Add(time.Duration(i) * time.Millisecond),
		})
	}
}

// Benchmark AddTask on a single TimerHeap under parallel contention
func BenchmarkTimerHeap_AddTask_Parallel(b *testing.B) {
	th := NewTimerHeap(0, 10000000, 0)
	now := time.Now()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			i++
			th.AddTask(&TimerTask{
				ID:     "task",
				FireAt: now.Add(time.Duration(i) * time.Millisecond),
			})
		}
	})
}

// Benchmark FireExpired task popping during flood condition
func BenchmarkTimerHeap_FireExpired_Flood(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		th := NewTimerHeap(0, 10000, 0)
		past := time.Now().Add(-1 * time.Hour)
		for j := 0; j < 5000; j++ {
			th.AddTask(&TimerTask{
				ID:     "expired-task",
				FireAt: past,
			})
		}
		b.StartTimer()
		th.FireExpired()
	}
}
