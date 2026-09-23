# latency-under-load

This is a personal project, like an assignment of sorts to test out **Rust** for system-level programming.

The project is high-concurrency, low-latency Level 2 (L2) crypto order book aggregator and arbitrage engine built in **Rust** and deployed on **Kubernetes** (Shouts to **Andela** for this. I registered to study Kubernetes from their end. The course was mainly taught by the **Cloud Native Computing Foundation (CNCF)**).

`latency-under-load` ingests real-time order book delta updates concurrently across multiple cryptocurrency exchanges (Binance and Coinbase), streams normalized market data through Kafka/Redpanda with Redis deduplication, maintains in-memory order books to detect cross-exchange arbitrage, and flushes historical order book updates into TimescaleDB, all without blocking the execution path.

Besides venturing into system-level programming, the project exists to answer one question end-to-end: **can this pipeline stay correct and fast under real, concurrent market data load, from raw exchange sockets all the way to durable storage, on infrastructure you'd actually run in production?**

---

## Table of Contents

- [Architecture](#-system-architecture)
- [Key Engineering Highlights](#key-engineering-highlights)
- [Tech Stack](#-tech-stack)
- [Getting Started](#-getting-started)
- [Performance](#-performance)
- [Roadmap](#roadmap)

---

## 🏛️ System Architecture

​                                 ┌────────────────────────────────┐
​                                 │                          WebSocket Clients                          │
​                                 │                 (Binance & Coinbase Streams)                │
​                                 └───────────────┬────────────────┘
​                                                                          │ Normalized Updates
​                                                                         ▼
​                                 ┌────────────────────────────────┐
​                                 │                      Kafka/Redpanda Producer                  │
​                                 │                           ("l2-market-data")                           │
​                                 └───────────────┬────────────────┘
​                                                                          │
​                                                                         ▼
​                                 ┌────────────────────────────────┐
​                                 │                        Kafka Consumer + Redis                  │
​                                 │                              (Deduplication)                            │
​                                 └───────────────┬────────────────┘
​                                                                          │
​                                                                         ▼
​                                 ┌────────────────────────────────┐
​                                 │                    Order Book Manager (OBM)                 │
​                                 │                   - Maintains In-Memory Books                │
​                                 │                   - Cross-Exchange Arbitrage                   │
​                                 └───────────────┬────────────────┘
​                                                                          │
​                                                                         ▼
​                                 ┌────────────────────────────────┐
​                                 │                     TimescaleDB Writer Task                     │
​                                 │                - Chunked Parameter Flushing                 │
​                                 └───────────────┬────────────────┘
​                                                                          │
​                                                                         ▼
​                                 ┌────────────────────────────────┐
​                                 │                     TimescaleDB (Hypertable)                   │
​                                 └────────────────────────────────┘

### Key Engineering Highlights

- **Multi-Asset & Multi-Exchange Ingestion**: Concurrent WebSocket connection handling depth streams across multiple assets (`BTC`, `ETH`, `SOL`, `ADA`, `DOGE`, `XRP`) across Binance and Coinbase.   
- **Real-Time Cross-Exchange Arbitrage**: In-memory L2 book management tracking best bids and asks across exchanges to alert on spread arbitrage opportunities in real time.   
- **Deduplicated Queueing**: Asynchronous event publishing to Redpanda/Kafka with Redis-backed deduplication filters to ensure idempotent pipeline processing.   
- **Optimized Time-Series Storage**: Automated batching into TimescaleDB with row chunking (1,000-row sub-batches) to enforce PostgreSQL’s 65,535 bind parameter limit during high-volume L2 depth flushes.
- **Zero-Data-Loss Graceful Shutdown**: `SIGTERM` and `SIGINT` OS signal handling configured to abort upstream networks, close MPSC channels, and drain in-flight buffers before termination. 
- **Zero-Lock State Aggregation:** Uses asynchronous message passing and lock-free concurrency patterns with `tokio` to process high-frequency WebSocket frames without thread contention.
- **Cloud-Native Deployment:** Fully containerized in Docker and orchestrated on Kubernetes (tested locally via Minikube), with self-bootstrapping schema.

---

## 🛠️ Tech Stack

| Layer         | Technology                                         |
| ------------- | -------------------------------------------------- |
| Language      | Rust (2024 edition)                                |
| Async Runtime | `tokio` (WebSockets, async I/O, channels, signals) |
| Event Broker  | Redpanda (Kafka protocol)                          |
| Dedup / Cache | Redis                                              |
| Database      | TimescaleDB / PostgreSQL                           |
|  Numerics     |  `rust_decimal` for exact financial calculations   |
| Infra         | Docker, Kubernetes (Minikube)                      |

---

## 🚀 Getting Started

### Prerequisites

- **Rust** 1.75+
- **Docker** & **Docker Compose**
- **Minikube** & **kubectl** (for Kubernetes deployment)

### Local Development (Bare Metal / Cargo)

1. **Clone the repository:**
   ```bash
   git clone https://github.com/Gyamps/latency-under-load.git
   cd latency-under-load
   
   docker-compose up -d
   cargo run --release
   ```

### ☸️ Kubernetes Deployment (Minikube)

1. **Start Minikube:**

   ```bash
   minikube start -p <profile-name>
   ```

2. **Build the image.** Pointing your shell at Minikube's Docker daemon works in theory:

   ```bash
   eval $(minikube docker-env -p <profile-name>)
   docker build -t <image-name>:latest .
   ```

   In practice, `minikube image build` is more reliable and what this project actually uses:

   ```bash
   minikube -p <profile-name> image build -t <image-name>:latest .
   ```

3. **Apply the Kubernetes manifests:**

   ```bash
   kubectl apply -f k8s/
   ```

4. **Verify deployment and stream logs:**
   ```bash
   kubectl get pods -n orderbook
   kubectl logs -n orderbook -l app=l2-aggregator -f
   ```

## Graceful Shutdown & Testing

The application captures Unix signals (`SIGTERM` from Kubernetes, `SIGINT` from terminal):   

- Aborts external WebSocket client connections to stop receiving new network messages.   
- Drops top-level MPSC channel senders.   
- Drains downstream queues (`kafka_consumer` $\rightarrow$ `order_book_manager` $\rightarrow$ `db_writer_task`).   
- Flushes remaining database buffers and C-core Kafka queues before exiting with code `0`.   

To verify shutdown behavior in Kubernetes:

```bash
kubectl delete pod -l app=l2-aggregator -n orderbook
```

## Querying Order Book Data

To inspect ingested time-series rows inside TimescaleDB:

```bash
kubectl exec -it timescaledb-0 -n orderbook -- psql -U postgres -d orderbook -c "
SELECT received_timestamp, symbol, price, qty, is_bid, is_snapshot
FROM order_book_updates
ORDER BY received_timestamp DESC
LIMIT 10;
"
```



## 📊 Features & Performance Design

- **Sequence Tracking & Gap Detection:** Tracks sequence IDs per market feed to flag dropped frames and trigger snapshot resynchronization automatically (mainly for Binance, Coinbase handles that automatically).
- **Memory-Optimized Order Books:** Depth maps utilize custom data structures optimized for fast insertion, deletion, and real-time top-of-book (BBO) lookups.

## Roadmap

- [ ] Add more exchanges (Kraken, OKX)
- [ ] Expose a WebSocket/gRPC feed for the consolidated book
- [ ] Grafana dashboard for live latency/throughput metrics
- [ ] Make the project a distributed system
