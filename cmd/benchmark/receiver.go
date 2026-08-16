package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type CallbackEvent struct {
	TaskID     string
	ReceivedAt time.Time
}

type MockCallbackReceiver struct {
	server       *http.Server
	listener     net.Listener
	addr         string
	events       sync.Map // map[string]time.Time
	eventCount   atomic.Int64
	latencyDelay time.Duration
	notifyChan   chan struct{}
}

func NewMockCallbackReceiver(port int, latencyDelay time.Duration) (*MockCallbackReceiver, error) {
	addr := fmt.Sprintf(":%d", port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	actualPort := listener.Addr().(*net.TCPAddr).Port
	receiver := &MockCallbackReceiver{
		listener:     listener,
		addr:         fmt.Sprintf("http://127.0.0.1:%d", actualPort),
		latencyDelay: latencyDelay,
		notifyChan:   make(chan struct{}, 100000),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/callback", receiver.handleCallback)

	receiver.server = &http.Server{
		Handler: mux,
	}

	return receiver, nil
}

func (r *MockCallbackReceiver) Start() {
	go func() {
		_ = r.server.Serve(r.listener)
	}()
}

func (r *MockCallbackReceiver) Stop() error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return r.server.Shutdown(ctx)
}

func (r *MockCallbackReceiver) URL() string {
	return r.addr + "/callback"
}

func (r *MockCallbackReceiver) handleCallback(w http.ResponseWriter, req *http.Request) {
	now := time.Now()

	if r.latencyDelay > 0 {
		time.Sleep(r.latencyDelay)
	}

	var payload struct {
		TimerID string `json:"timer_id"`
		Message string `json:"message"`
	}

	if err := json.NewDecoder(req.Body).Decode(&payload); err == nil && payload.TimerID != "" {
		r.events.Store(payload.TimerID, now)
		r.eventCount.Add(1)

		select {
		case r.notifyChan <- struct{}{}:
		default:
		}
	}

	w.WriteHeader(http.StatusOK)
}

func (r *MockCallbackReceiver) GetReceivedTime(taskID string) (time.Time, bool) {
	val, ok := r.events.Load(taskID)
	if !ok {
		return time.Time{}, false
	}
	return val.(time.Time), true
}

func (r *MockCallbackReceiver) TotalReceived() int {
	return int(r.eventCount.Load())
}

// WaitForCount waits until expectedCount callbacks have been received, or the timeout expires.
func (r *MockCallbackReceiver) WaitForCount(expectedCount int, timeout time.Duration) int {
	deadline := time.After(timeout)
	for {
		if r.TotalReceived() >= expectedCount {
			return r.TotalReceived()
		}
		select {
		case <-deadline:
			return r.TotalReceived()
		case <-r.notifyChan:
			if r.TotalReceived() >= expectedCount {
				return r.TotalReceived()
			}
		}
	}
}
