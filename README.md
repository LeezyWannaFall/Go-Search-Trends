# Go Search Trends

A high-throughput Go backend service that consumes search events from Kafka and exposes the **Top-N trending queries over a rolling 5-minute window**.

The project focuses on read-heavy workloads, low-latency in-memory aggregation, concurrent access, and explicit engineering trade-offs.

## Highlights

- Real-time search event processing with **Apache Kafka**
- Rolling 5-minute Top-N query aggregation
- Counts **unique users per query** instead of raw request volume to reduce simple spam/bot amplification
- In-memory minute buckets optimized for fast reads
- Concurrent access with `sync.RWMutex`
- Runtime stop-list management through REST endpoints
- Docker Compose setup for local development

## Tech Stack

- **Go**
- **Apache Kafka**
- **Docker / Docker Compose**
- **REST / JSON**
- Go standard library synchronization primitives

## How It Works

Search events are published to the Kafka topic `search-events`.

Example event:

```json
{
  "query": "golang",
  "user_id": "user-42",
  "timestamp": "2026-05-25T12:00:00Z"
}
```

The service consumes the events, places them into minute-based in-memory buckets, and exposes the most popular queries from the active 5-minute window.

Popularity is based on the number of **unique `user_id` values**, not on the total number of requests. A single user sending the same query thousands of times within a minute therefore contributes a weight of `1`.

## Architecture

```text
                         +------------------+
                         |  Search Events   |
                         +--------+---------+
                                  |
                                  v
                         +------------------+
                         |      Kafka       |
                         |  search-events   |
                         +--------+---------+
                                  |
                                  v
                    +---------------------------+
                    |     Go Consumer / API     |
                    |                           |
                    |  Event validation         |
                    |  Minute bucket storage    |
                    |  Unique-user aggregation  |
                    |  Stop-list filtering      |
                    +-------------+-------------+
                                  |
                    +-------------+-------------+
                    |                           |
                    v                           v
             +-------------+            +-------------+
             | GET /top    |            | Stop-list   |
             | Top-N API   |            | REST API    |
             +-------------+            +-------------+
```

The main in-memory structure is conceptually:

```go
map[int64]map[string]map[string]struct{}
```

where:

```text
minute -> query -> set of unique user IDs
```

Example:

```text
minute N-2:
  golang -> {u1, u2, u3}
  kafka  -> {u1, u5}

minute N-1:
  golang -> {u2, u4}
  redis  -> {u3}

minute N:
  golang -> {u1}
  docker -> {u2, u6, u7}
```

The outer key is `unix_timestamp / 60`.

## Why Minute Buckets?

The service only needs a short rolling history.

Using minute buckets gives direct access to the relevant time interval without storing every event indefinitely. It also makes old data inexpensive to discard.

A map was chosen instead of a slice because events arrive with timestamps and the corresponding minute bucket can be accessed directly without scanning for an index.

## Concurrency

The workload is expected to be heavily read-oriented, with reads occurring roughly **10-50x more often than writes**.

The in-memory store uses `sync.RWMutex`:

- multiple `GET /top` requests can read concurrently;
- event ingestion acquires an exclusive lock only when modifying state.

This keeps the synchronization model simple while allowing concurrent readers.

## Top-N Strategy

The project intentionally does **not** maintain a heap or priority queue on every write.

A heap would make Top-N reads cheaper, but every incoming event could require maintaining ordering state. For a workload with significantly more reads than writes, the current implementation favors simple writes and computes ordering when Top-N data is requested.

This is a deliberate trade-off rather than an assumption that one data structure is universally faster.

## Why In-Memory Instead of Redis?

The trending data is intentionally short-lived and only relevant for a few minutes.

For the scope of this service:

- persistence is not required;
- local memory avoids an additional network hop;
- the implementation stays small and predictable.

A distributed deployment would require a different design, such as shared state, partitioned aggregation, or an external data store. The current implementation optimizes for a single service instance and low read latency.

## Event Time vs. Processing Time

The rolling window uses the event's `timestamp`, rather than the time at which the Kafka consumer receives it.

This means the ranking reflects when the search actually happened instead of when it happened to reach the consumer.

Trade-off: a producer with an incorrect clock or a significantly delayed event can place data into the wrong bucket. For this project, event time was chosen because it better represents real search activity.

## Basic Abuse Resistance

A plain counter such as:

```go
map[string]int
```

would allow one client to increase the rank of a query by repeatedly sending the same event.

Instead, each query stores a set of user IDs for each minute:

```go
map[string]struct{}
```

The query weight is therefore the number of unique users in the bucket.

This protects against repeated events from one fixed user ID, but it does **not** solve distributed abuse from many identities. More advanced protection could include velocity checks, IP-based limits, or behavioral analysis.

## API

### `GET /top`

Returns the Top-N search queries from the active rolling window.

Query parameter:

| Parameter | Type | Default | Description |
|---|---:|---:|---|
| `n` | int | `10` | Number of queries to return |

Example:

```bash
curl "http://localhost:8080/top?n=5"
```

Response:

```json
[
  {"query": "golang", "count": 42},
  {"query": "kafka", "count": 31},
  {"query": "docker", "count": 17}
]
```

If there is no data in the active window, the endpoint currently returns `null`.

### `POST /stoplist/{word}`

Adds a word to the stop list. Matching queries are excluded immediately without restarting the service.

```bash
curl -X POST "http://localhost:8080/stoplist/spam"
```

Response:

```text
204 No Content
```

### `DELETE /stoplist/{word}`

Removes a word from the stop list.

```bash
curl -X DELETE "http://localhost:8080/stoplist/spam"
```

### `GET /stoplist`

Returns the current stop list.

```bash
curl "http://localhost:8080/stoplist"
```

Example response:

```json
["spam", "ads"]
```

## Running Locally

Requirements:

- Docker
- Docker Compose

Clone and start the service:

```bash
git clone https://github.com/LeezyWannaFall/Go-Search-Trends.git
cd Go-Search-Trends
docker compose up --build
```

The API will be available at:

```text
http://localhost:8080
```

## Data Contract

Kafka topic:

```text
search-events
```

Message schema:

| Field | Type | Required | Description |
|---|---|:---:|---|
| `query` | string | Yes | Search query text |
| `user_id` | string | Yes | User identifier used for unique-user counting |
| `timestamp` | RFC 3339 string | Yes | Time at which the search event occurred |

## Known Trade-offs

### Rolling-window precision

The implementation uses minute granularity rather than storing every event at second-level precision.

This keeps the model simple and memory-efficient, but the effective window is approximate rather than an exact 300-second interval.

### Distributed scaling

The aggregation state lives in process memory, so multiple independent replicas would not automatically share ranking state.

A production distributed version would need an explicit partitioning/aggregation strategy or shared storage.

### Abuse prevention

Unique-user counting prevents trivial repeated requests from a single identity, but it does not prevent coordinated or distributed abuse.

### Producer clock accuracy

Because ranking uses event time, incorrect producer clocks can affect bucket placement.