package timer

import (
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"
)

func (t *Timer) StartServer(addr string) error {
	mux := http.NewServeMux()

	mux.HandleFunc("/api", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		id := r.FormValue("id")
		fireInSecStr := r.FormValue("timer_time")
		callbackURL := r.FormValue("callback_url")

		if id == "" || fireInSecStr == "" || callbackURL == "" {
			http.Error(w, "Missing required parameters: id, timer_time, or callback_url", http.StatusBadRequest)
			return
		}

		fireInSec, err := strconv.ParseFloat(fireInSecStr, 64)
		if err != nil || fireInSec < 0 {
			http.Error(w, "Invalid timer_time, must be a non-negative number", http.StatusBadRequest)
			return
		}

		task := &TimerTask{
			ID:          id,
			FireAt:      time.Now().Add(time.Duration(fireInSec * float64(time.Second))),
			CallBackURL: callbackURL,
		}

		heapID, ok := t.AddTask(task)
		if !ok {
			http.Error(w, fmt.Sprintf("Heap %d queue capacity reached", heapID), http.StatusServiceUnavailable)
			return
		}
		log.Printf("Routed task %s to heap %d (expires in %.2fs)", task.ID, heapID, fireInSec)

		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintf(w, "success: routed to heap %d\n", heapID)
	})

	server := http.Server{
		Addr:    addr,
		Handler: mux,
	}

	log.Printf("Listening for HTTP API requests on %s...", addr)
	return server.ListenAndServe()
}
