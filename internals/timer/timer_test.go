package timer

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestTimer_RoundRobinRouting(t *testing.T) {
	heapCount := 4
	tm := NewTimer(heapCount, 100, 1)

	counts := make(map[int]int)
	for i := 0; i < 16; i++ {
		heapID, ok := tm.AddTask(&TimerTask{
			ID:     fmt.Sprintf("task-%d", i),
			FireAt: time.Now().Add(1 * time.Hour),
		})
		if !ok {
			t.Fatalf("failed to add task %d", i)
		}
		counts[heapID]++
	}

	for id := 0; id < heapCount; id++ {
		if counts[id] != 4 {
			t.Errorf("heap %d received %d tasks, expected 4", id, counts[id])
		}
	}
}

func TestTimer_EndToEndCallback(t *testing.T) {
	var received atomic.Int32
	var receivedTaskID string
	var mu sync.Mutex

	// Mock callback HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]string
		if err := json.NewDecoder(r.Body).Decode(&payload); err == nil {
			mu.Lock()
			receivedTaskID = payload["timer_id"]
			mu.Unlock()
		}
		received.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	tm := NewTimer(2, 100, 2)
	tm.Start()

	taskID := "test-e2e-task"
	_, ok := tm.AddTask(&TimerTask{
		ID:          taskID,
		FireAt:      time.Now().Add(50 * time.Millisecond),
		CallBackURL: server.URL,
	})
	if !ok {
		t.Fatalf("failed to add task")
	}

	// Wait for callback to be triggered
	deadline := time.After(2 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for callback notification")
		default:
			if received.Load() > 0 {
				mu.Lock()
				id := receivedTaskID
				mu.Unlock()
				if id != taskID {
					t.Errorf("expected task ID %s, got %s", taskID, id)
				}
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func benchmarkMultiHeapAddTask(b *testing.B, heapCount int) {
	tm := NewTimer(heapCount, 10000000, 0)
	now := time.Now()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			i++
			tm.AddTask(&TimerTask{
				ID:     "task",
				FireAt: now.Add(time.Duration(i) * time.Millisecond),
			})
		}
	})
}

func BenchmarkTimer_AddTask_1Heap(b *testing.B) {
	benchmarkMultiHeapAddTask(b, 1)
}

func BenchmarkTimer_AddTask_2Heaps(b *testing.B) {
	benchmarkMultiHeapAddTask(b, 2)
}

func BenchmarkTimer_AddTask_4Heaps(b *testing.B) {
	benchmarkMultiHeapAddTask(b, 4)
}

func BenchmarkTimer_AddTask_8Heaps(b *testing.B) {
	benchmarkMultiHeapAddTask(b, 8)
}

func BenchmarkTimer_AddTask_16Heaps(b *testing.B) {
	benchmarkMultiHeapAddTask(b, 16)
}
