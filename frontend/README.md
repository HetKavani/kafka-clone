# KafkaX Console

KafkaX Console is a professional distributed-systems monitoring and management dashboard for the **KafkaX** distributed event streaming platform. 

It provides real-time visibility into the health, throughput, replication states, and consumer group positions across all active brokers.

---

## Available Features

1. **Dashboard**: Unified control panel showing cluster health state, active broker list, topic inventory, active consumer counts, aggregate lag, latency charts (Recharts), and a live cluster activity stream.
2. **Brokers**: Details on all registered brokers, their status, hosts/ports, role stats (number of partitions led vs. replicated), and lists of hosted partition shards.
3. **Topics**: List of configured topics and partition distributions. Users can create new topics (specifying partition shards) or inspect individual partition leader-replica details, offsets, and replica status.
4. **Consumer Groups**: Detailed inspection of consumer groups showing active members, assigned partition offsets, and computed lag (difference between log end offset and committed offset).
5. **Event Explorer & Replay**:
   - **Explorer**: Query partition log files starting at index 0, publish test events using customizable `acks` levels (`none`, `leader`, `all`), and inspect payloads.
   - **Replay**: Replay event streams from various strategies (`earliest`, `latest`, `specific offset`, or `timestamp`).
6. **Cluster Topology Graph**: Visualizes the hierarchical relationship of the cluster (`Broker -> Partition -> Leader / Replica`) dynamically using **React Flow**, marking online leaders, in-sync replicas, degraded nodes, and offline systems.

---

## Configuration & Environment Variables

The console is configured via Vite environment variables. Create a `.env` file in the `frontend` folder:

```bash
VITE_API_URL=http://localhost:8000
```

- **VITE_API_URL**: Points to the main FastAPI cluster gateway. Defaults to `http://localhost:8000`.

---

## Getting Started

1. **Install dependencies:**
   ```bash
   npm install
   ```

2. **Start the local development server:**
   ```bash
   npm run dev
   ```
   Open [http://localhost:5173](http://localhost:5173) in your browser.

3. **Build the production bundle:**
   ```bash
   npm run build
   ```

---

## Backend API Endpoints Required

The console relies on the following standard KafkaX REST API endpoints:

- `GET /api/v1/cluster` — Retrieves summary counts and overall health.
- `GET /api/v1/cluster/brokers` — Lists all registered active and inactive brokers.
- `GET /api/v1/cluster/partitions` — Lists partition-level leadership and replicas.
- `GET /api/v1/topics` — Lists all topics.
- `POST /api/v1/topics/` — Creates a new topic.
- `DELETE /api/v1/topics/{topic}` — Deletes a topic.
- `POST /api/v1/topics/{topic}/publish` — Publishes events to the cluster.
- `GET /api/v1/topics/{topic}/events` — Fetches events (with strategies for explorer/replay).
- `GET /api/v1/consumer-groups` — Lists consumer groups.
- `GET /api/v1/consumer-groups/{group}/lag` — Retrieves lag statistics.
- `GET /api/v1/metrics` — Retrieves aggregate cluster latency and message throughput.
- `GET /api/v1/health` — Heartbeat health check of database and Redis.
