# Ride-Sharing: Kafka + Temporal demo

A tiny, teaching-oriented codebase that shows **how the Kafka protocol works**
and how to bridge a Kafka topic into a **Temporal workflow** — using a
ride-sharing scenario. Everything runs in Docker.

```
                POST /rides
  you  ─────────────────────────►  producer (HTTP API)
                                        │  Kafka WRITE (key = city)
                                        ▼
                                ┌──────────────────┐
                                │  Kafka topic     │   "ride-requests"
                                │  3 partitions    │   key=city → partition
                                └──────────────────┘
                                        │  Kafka FETCH (consumer group)
                                        ▼
                                   consumer  ──ExecuteWorkflow──►  Temporal
                                                                     │
                                                            worker runs RideWorkflow:
                                                              1. ValidateRider
                                                              2. MatchDriver
                                                              3. CalculateFare
                                                              4. ConfirmRide
```

## What each piece teaches

| Component | Kafka / Temporal concept it demonstrates |
|-----------|------------------------------------------|
| `producer` | A **publisher**: writes JSON to a topic. Uses the **city as the partition key** so same-city rides stay ordered on one partition. |
| `kafka` topic | **Topics & partitions** — 3 partitions, `RequireAll` acks. |
| `consumer` | A **consumer group** member. Reads with `FetchMessage`, commits **offsets** manually for **at-least-once** delivery. |
| `worker` | A Temporal **worker** polling a task queue, running the **workflow + activities** durably (auto-retries, survives restarts). |

## Run it

```bash
docker compose up --build
```

The `kafka-init` service pre-creates the topic, so the consumer gets all 3
partitions on first boot — no manual restart needed. Wait until `ride_worker`
and `ride_consumer` log that they've started, then:

```bash
# Publish a ride request (the "simple publisher")
curl -X POST localhost:8081/rides \
  -H 'content-type: application/json' \
  -d '{"rider_id":"rider-1","city":"dhaka","pickup":"Gulshan","dropoff":"Banani"}'
```

Watch the logs:

```bash
docker compose logs -f producer consumer worker
```

You'll see: producer publishes → consumer reads (partition + offset) →
consumer starts a workflow → worker runs the 4 activities → ride CONFIRMED.

### See the Kafka protocol in action

- **Kafka UI**:  http://localhost:8088  — browse the topic, its 3 partitions,
  per-partition offsets, and the `ride-workers` consumer group lag.
- **Temporal Web UI**: http://localhost:8244 — see each `ride-<id>` workflow,
  its activity timeline, retries, and inputs/outputs. (Bundled into the
  `temporal server start-dev` container; serves the container's port 8233.)

### Demonstrate partitioning & ordering

Send several rides for different cities and watch them spread across
partitions (the **key** decides the partition):

```bash
for c in dhaka chittagong sylhet dhaka khulna dhaka; do
  curl -s -X POST localhost:8081/rides -H 'content-type: application/json' \
    -d "{\"rider_id\":\"r\",\"city\":\"$c\",\"pickup\":\"A\",\"dropoff\":\"B\"}" ; echo
done
```

All `dhaka` messages hash to the **same partition** (ordered); other cities
land elsewhere. Open Kafka UI → topic `ride-requests` → Messages to confirm.

### Demonstrate consumer scaling

Run a second consumer; Kafka rebalances the 3 partitions across both:

```bash
docker compose up -d --scale consumer=2
```

### Demonstrate durability (Temporal)

Kill the worker mid-flight and restart — the workflow resumes from the last
completed activity instead of starting over:

```bash
docker compose restart worker
```

## Temporal architecture

Temporal separates **what** runs (your workflow/activity code) from **where the
state lives** (the Temporal Service). Your code is just a worker process; the
Service is the durable brain that remembers every step.

```
   consumer ──ExecuteWorkflow(req)──►  ┌─────────────────────────────────────┐
                                       │        TEMPORAL SERVICE             │
   (Temporal Go SDK client)            │                                     │
                                       │  Frontend  ─►  History  ─►  Persist │
                                       │  (gRPC      (event log    (SQLite in │
                                       │   :7233)     per run)      dev mode) │
                                       │      │                              │
                                       │   Matching ──► Task Queue           │
                                       │              "ride-task-queue"      │
                                       └──────────────┬──────────────────────┘
                                                      │ long-poll for tasks
                                                      ▼
                                        ┌───────────────────────────┐
                                        │  ride_worker (our Go bin)  │
                                        │  RegisterWorkflow(...)     │
                                        │  RegisterActivity(...)     │
                                        │                            │
                                        │  RideWorkflow orchestrates │
                                        │   → ValidateRider          │
                                        │   → MatchDriver            │
                                        │   → CalculateFare          │
                                        │   → ConfirmRide            │
                                        └───────────────────────────┘
```

**The pieces (all bundled by `temporal server start-dev`):**

| Part | Role |
|------|------|
| **Frontend** | gRPC API (`:7233`) the SDK client talks to — `ExecuteWorkflow`, queries, signals. |
| **History** | Records every workflow as an **event log** (started, activity scheduled, completed…). This log is what makes execution *durable* and *replayable*. |
| **Matching** | Hands buffered tasks to whichever worker is polling the matching **Task Queue**. |
| **Persistence** | Stores the event logs. Dev mode uses in-memory SQLite; production uses PostgreSQL/Cassandra. |
| **Worker** (`ride_worker`) | *Your* process. Long-polls the task queue, runs workflow + activity code, reports results back. Stateless — all state lives in History. |
| **Web UI** | `:8233` (mapped to host **8244**) — visualises every run's event timeline. |

**Why durability works:** the worker holds no state. Each step a workflow takes
is appended to the History event log *before* it runs. If `ride_worker` crashes
after `MatchDriver` but before `CalculateFare`, Temporal simply replays the log
on any available worker and resumes at `CalculateFare` — activities already done
are *not* re-run. That's what `docker compose restart worker` demonstrates.

**Flow in this demo:** `consumer` (Temporal *client*) calls `ExecuteWorkflow`
→ Frontend writes a "workflow started" event to History → Matching enqueues a
workflow task on `ride-task-queue` → `worker` polls it, runs `RideWorkflow`,
which schedules the four activities one by one, each recorded in History → run
ends as **Completed**, visible in the Web UI.

## Layout

```
cmd/producer    HTTP API → Kafka publisher
cmd/consumer    Kafka consumer group → starts Temporal workflows
cmd/worker      Temporal worker (workflow + activities)
internal/kafka  Producer / Consumer wrappers (segmentio/kafka-go)
internal/temporal  Client, RideWorkflow, Activities
internal/model  Shared message structs
```

## Containers (docker compose)

| Service | Port (host) | Purpose |
|---------|-------------|---------|
| `kafka` | 9092 | Single-node broker, KRaft mode (no Zookeeper). |
| `kafka-init` | — | One-shot: creates `ride-requests` with 3 partitions, then exits. |
| `kafka-ui` | 8088 | Web UI for topics/partitions/offsets/consumer groups. |
| `temporal` | 7244→7233, 8244→8233 | Temporal dev server + bundled Web UI. |
| `worker` | — | Runs `RideWorkflow` + activities. |
| `consumer` | — | Kafka consumer group → `ExecuteWorkflow`. |
| `producer` | 8081→8080 | HTTP publisher (`POST /rides`). |

## Tear down

```bash
docker compose down
```
