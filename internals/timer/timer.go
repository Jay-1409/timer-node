package timer

import "sync/atomic"

type Timer struct {
	Heaps         []*TimerHeap
	nextHeapIndex uint32
}

func NewTimer(heapCount int, queueSize int, workerCount int) *Timer {
	timer := &Timer{}
	for i := 0; i < heapCount; i = i + 1 {
		h := NewTimerHeap(i, queueSize, workerCount)
		timer.Heaps = append(timer.Heaps, h)
	}
	timer.nextHeapIndex = 0
	return timer
}

func (t *Timer) Start() {
	for _, h := range t.Heaps {
		go h.Run()
	}
}

/**
	Round robin load balancing on the heaps and returns the chosen heap id
	We are using cpu instructions for adding 1 thus we dont need to worry about
	sync here.
*/
func (t *Timer) AddTask(task *TimerTask) (int, bool) {
	idx := atomic.AddUint32(&t.nextHeapIndex, 1)
	curHeapIndex := int(idx % uint32(len(t.Heaps)))
	chosenHeap := t.Heaps[curHeapIndex]
	ok := chosenHeap.AddTask(task)
	return chosenHeap.ID, ok
}