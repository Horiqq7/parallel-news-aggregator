# Parallel News Aggregator

This project is a high-performance, multithreaded Java engine designed to process, filter, and aggregate large volumes of JSON news articles. It leverages a Map-Reduce architecture optimized with the Replicated Workers model to handle data efficiently and eliminate I/O bottlenecks.

## Core Architecture

The processing pipeline is structured into 4 distinct parallel phases, synchronized using a `CyclicBarrier`:

1. **Map Phase (Read & Index):** Threads dynamically fetch files using an atomic counter and parse the JSON articles using the Jackson library. Articles are loaded into local memory, and global frequency maps (`ConcurrentHashMap`) are updated concurrently to count title and UUID occurrences.
2. **Filter Phase (Consolidation):** After a barrier synchronization, each thread validates its local list against the global counters. Only globally unique articles are kept, minimizing contention on shared resources.
3. **Process Phase (Parallel Extraction):** Threads independently process their valid articles to extract and compute statistics (e.g., author frequencies, languages, keyword occurrences) without interfering with each other.
4. **Reduce Phase (Write):** After sorting and aggregating all partial results, a single deterministic thread writes the final structured output to disk.

## Concurrency Mechanisms

* **Thread Pool:** A fixed number of persistent worker threads.
* **Synchronization:** `CyclicBarrier` ensures all threads complete a specific phase before moving to the next, preventing race conditions during global data reads.
* **Thread-Safe Data Structures:** Extensive use of `ConcurrentHashMap` and `AtomicInteger` to aggregate statistics (keywords, authors, categories) without explicit locking mechanisms.

## Performance Analysis

The application is highly scalable. Based on automated testing methodologies with large datasets:
* **Execution Time:** Decreased from ~9.31 seconds (sequential) to ~3.94 seconds on 4 threads.
* **Speedup:** Achieved a 2.36x speedup on 4 threads. 
* **Amdahl's Law Limitations:** The performance scaling is bound by the sequential I/O operations (reading files from disk and writing the final aggregations), which cannot be fully parallelized.

## Build and Execute

The project uses a standard `Makefile` and includes the necessary Jackson `.jar` libraries for JSON parsing.

**Compilation:**
```bash
make build
make run ARGS="<num_threads> <input_articles_file> <auxiliary_file>"