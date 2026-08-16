.PHONY: all build test bench bench-unit bench-throughput bench-accuracy bench-flood bench-scaling bench-all clean

GO := /opt/homebrew/bin/go
ifeq (, $(shell which $(GO) 2>/dev/null))
	GO := go
endif

all: build

build:
	@mkdir -p bin
	$(GO) build -o bin/atimer ./cmd/atimer
	$(GO) build -o bin/benchmark ./cmd/benchmark

test:
	$(GO) test -v -race ./internals/timer/...

bench-unit:
	$(GO) test -bench=. -benchmem ./internals/timer/...

bench-throughput: build
	./bin/benchmark -scenario throughput -requests 10000 -concurrency 50

bench-accuracy: build
	./bin/benchmark -scenario accuracy -requests 2000 -concurrency 20 -delay 1s

bench-flood: build
	./bin/benchmark -scenario flood -requests 2000 -concurrency 50 -delay 1s

bench-scaling: build
	./bin/benchmark -scenario scaling -requests 10000 -concurrency 50

bench-grid: build
	@mkdir -p benchmark_results
	./bin/benchmark -scenario grid -heaps-list 1,2,4,8,16,32 -workers-list 1,2,4,8,16 -requests 2000 -delay 1s -plot-file benchmark_results/drift_plot.html -csv-file benchmark_results/drift_grid.csv

bench-all: build
	@mkdir -p benchmark_results
	./bin/benchmark -scenario all -requests 5000 -concurrency 50 -report-file benchmark_results/benchmark_report.md

clean:
	rm -rf bin/ benchmark_results/
