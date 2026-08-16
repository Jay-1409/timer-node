# atimer

> A highly performant, sharded clock node capable of receiving multiple timer requests and firing asynchronous callbacks when they expire.

![License](https://img.shields.io/badge/license-Apache2.0-blue)
![Language](https://img.shields.io/badge/language-Go1.25-blue)

---

## What's in there for you
- **Automated Callbacks**: Schedules task timers and automatically pings your server when they expire.
- **CPU & Battery Friendly**: Runs very efficiently 
- **Reliable Handling**: Capable of keeping track of many active timers at the same time.
- **Simple API**: Set up and trigger a timer using a single HTTP request.

---

## Features

* **High-Throughput Scheduling**: Handles thousands of concurrent active timers smoothly by load-balancing them internally.

---

## Use Cases

- **Serverless Apps**: Pings serverless functions (like AWS Lambda or Vercel) that cannot run background timers.

---

## Quick Start Guide
### 0. Clone the project if not already done.
### 1. Build and Run
Build the project using standard Go compiler tools:

```bash
go build ./cmd/atimer
```

Start the server using CLI flags to configure the port, number of heaps, and worker threads per heap:

```bash
./atimer -port 8080 -heaps 4 -workers 2
```

### 2. Schedule a Task
Send a `POST` request with form values to the `/api` endpoint:

```bash
curl -X POST http://localhost:8080/api \
  -d "id=task123" \
  -d "timer_time=10" \
  -d "callback_url=http://example.com/callback"
```

---

## Documentation

- API Endpoint reference [here](docs/endpoints.md)
- Timer Architecture details [here](docs/architecture.md)
- Benchmarking & Performance Guide [here](docs/benchmarks.md)
- Bruno collection [here](bruno/)
- System whiteboard drawings (excalidraw) [here](docs/whiteboard.excalidraw)
- Just a journal about this project [here](docs/journal.md)
---

## 📄 License

This project is licensed under the Apache 2.0 License. See the [LICENSE](LICENSE) file for details.
