package timer

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"time"
)

type TimerEventHandler struct {
	ID          int
	EventQueue  chan *TimerTask
	WorkerCount int
	Client      *http.Client
}

/** 
	The timer event handler will inherit the same id as of the heap that it is an event handler for. 
*/
func NewTimerEventHandler(id int, queueSize int, workerCount int) *TimerEventHandler {
	return &TimerEventHandler{
		ID:          id,
		EventQueue:  make(chan *TimerTask, queueSize),
		WorkerCount: workerCount,
		Client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (h *TimerEventHandler) Handler() {
	for i := 0; i < h.WorkerCount; i++ {
		go func() {
			for task := range h.EventQueue {
				h.ShootEvent(task)
			}
		}()
	}
}

func (h *TimerEventHandler) Dispatch(task *TimerTask) {
	if h.WorkerCount == 0 {
		return
	}
	select {
	case h.EventQueue <- task:
	default:
		log.Printf("EventHandler %d queue full, dropped task %s", h.ID, task.ID)
	}
}

func (h *TimerEventHandler) ShootEvent(task *TimerTask) {
	payload := map[string]string{
		"timer_id": task.ID,
		"message":  "timer expired",
	}

	payloadData, err := json.Marshal(payload)
	
	if err != nil {
		log.Printf("Error marshaling payload for task %s: %v", task.ID, err)
		return
	}

	resp, err := h.Client.Post(task.CallBackURL, "application/json", bytes.NewReader(payloadData))
	
	if err != nil {
		log.Printf("Error sending notification for task %s: %v", task.ID, err)
		return
	}

	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		log.Printf("Received non-2xx status code %d for task %s", resp.StatusCode, task.ID)
		return
	}
	
	log.Printf("Successfully sent notification for task %s to %s", task.ID, task.CallBackURL)
}
