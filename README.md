# KafkaOMS — Event-Driven Order Management System

> A Kafka-based trading workflow demonstrating event-driven microservices architecture with real-time order processing, risk validation, execution simulation, and portfolio tracking.

## 🎯 Project Aim

Build an **event-sourced trading system** where every state change flows through Kafka as an immutable event. The goal is to learn Kafka fundamentals through a realistic financial use case:

**Flow:** `Strategy Decision → Order → Risk Validation → Execution → Fill → Portfolio Update`

### Why This Project?

- 📚 **Learn Kafka properly** — topics, partitions, consumer groups, offset management
- 🏗️ **Master event-driven architecture** — decoupled services, event sourcing, replay semantics
- 💰 **Real-world domain** — trading workflows mirror production OMS systems
- 🔄 **State reconstruction** — rebuild portfolio state by replaying events from offset 0

---

## 🏛️ Architecture

```
┌─────────────────┐
│  Market Data    │──┐
│   Producer      │  │
└─────────────────┘  │
                     ▼
                ┌─────────┐
                │  Kafka  │
                └─────────┘
                     │
      ┌──────────────┼──────────────┐
      ▼              ▼              ▼
┌──────────┐   ┌──────────┐   ┌──────────┐
│ Strategy │   │   Risk   │   │Execution │
│ Service  │──▶│ Service  │──▶│ Service  │
└──────────┘   └──────────┘   └──────────┘
                                    │
                                    ▼
                              ┌──────────┐
                              │Portfolio │
                              │ Service  │
                              └──────────┘
                                    │
                                    ▼
                              ┌──────────┐
                              │ Monitor  │
                              │ Service  │
                              └──────────┘
```

### Services

1. **MarketDataProducer** — Publishes simulated stock prices (AAPL, GOOGL, MSFT)
2. **StrategyService** — Submits random buy/sell orders based on market data
3. **RiskService** — Pre-trade risk checks (max order size, position limits, no shorting)
4. **ExecutionService** — Simulates order fills with slippage and fees
5. **PortfolioService** — Tracks positions, cash, realized/unrealized PnL
6. **MonitorService** — Live terminal dashboard showing portfolio state

### Kafka Topics

| Topic | Purpose | Producer | Consumer |
|-------|---------|----------|----------|
| `market-data` | Stock price ticks | MarketDataProducer | Strategy, Execution, Monitor |
| `orders-submitted` | New orders | StrategyService | RiskService, ExecutionService |
| `orders-approved` | Approved orders | RiskService | ExecutionService |
| `orders-rejected` | Rejected orders | RiskService | MonitorService |
| `fills` | Executed trades | ExecutionService | PortfolioService, MonitorService |
| `portfolio-updated` | Position snapshots | PortfolioService | MonitorService |
| `risk-alerts` | Risk violations | (future) | (future) |

---

## 🚀 Quick Start

### Prerequisites

- **Java 21+** (check: `java -version`)
- **Maven 3.8+** (check: `mvn -version`)
- **Docker Desktop** running (check: `docker ps`)

### 1. Start Kafka Infrastructure

```bash
docker compose up -d
```

This starts:
- Zookeeper (coordination)
- Kafka Broker (message backbone)
- Kafka UI at http://localhost:8080 (topic browser)

Wait ~30 seconds for Kafka to be ready.

### 2. Build All Services

```bash
mvn clean install
```

### 3. Start Services (each in a separate terminal)

```bash
# Terminal 1: Market data
cd market-data-producer && mvn exec:java

# Terminal 2: Strategy
cd strategy-service && mvn exec:java

# Terminal 3: Risk engine
cd risk-service && mvn exec:java

# Terminal 4: Execution engine
cd execution-service && mvn exec:java

# Terminal 5: Portfolio tracker
cd portfolio-service && mvn exec:java

# Terminal 6: Live dashboard
cd monitor-service && mvn exec:java
```

### 4. Watch It Run

- **Terminal 6** shows live portfolio dashboard (positions, PnL, recent events)
- **Kafka UI** (http://localhost:8080) shows all messages flowing through topics
- Watch orders flow through the pipeline end-to-end

---

## 📊 What You'll See

When all services are running:

1. **Market Data** publishes prices every 2 seconds
2. **Strategy** submits random orders every ~5 ticks
3. **Risk** approves/rejects orders based on rules:
   - Max order size: 100 shares
   - Max position per symbol: 500 shares
   - Long-only (no shorting)
4. **Execution** fills approved orders at market price + slippage + $1 fee
5. **Portfolio** updates positions and calculates PnL
6. **Monitor** displays everything in a live dashboard

### Sample Monitor Output

```
╔════════════════════════════════════════════════════════════╗
║                    PORTFOLIO MONITOR                       ║
╠════════════════════════════════════════════════════════════╣
║  Cash:           $98,456.20                                ║
║  Portfolio Val:  $101,543.80                               ║
║  Total PnL:      +$3,087.60 (+3.09%)                       ║
╠════════════════════════════════════════════════════════════╣
║  POSITIONS                                                 ║
║  AAPL    120 shares @ $187.50  R-PnL: +$240  U-PnL: +$180 ║
║  GOOGL    50 shares @ $142.20  R-PnL: +$120  U-PnL: -$50  ║
║  MSFT     80 shares @ $380.10  R-PnL: +$180  U-PnL: +$240 ║
╠════════════════════════════════════════════════════════════╣
║  RECENT EVENTS                                             ║
║  ✅ FILL: ord_1234 BUY AAPL 10 @ $187.50                   ║
║  ❌ REJECTED: ord_1235 (position limit exceeded)           ║
╚════════════════════════════════════════════════════════════╝
```

---

## 🔍 Observability

### Kafka UI (http://localhost:8080)

- Browse all topics and messages
- Check consumer lag per service
- Inspect partition distribution
- View message payloads (JSON)

### Logs

Each service outputs structured logs with:
- Event IDs for tracing
- Emoji indicators (💰 fills, ❌ rejections, ✅ approvals)
- Partition and offset metadata

---

## 🎓 Key Kafka Concepts Demonstrated

| Concept | Where to See It |
|---------|----------------|
| **Topics & Partitions** | Kafka UI → Topics tab |
| **Consumer Groups** | Each service has unique `group.id` |
| **Offset Management** | Kafka UI → Consumers tab (lag tracking) |
| **Event Sourcing** | Stop PortfolioService, restart → replays from offset 0 |
| **Idempotency** | Restart any service → no duplicate processing |
| **Decoupling** | Kill ExecutionService → others keep running |
| **Ordering Guarantees** | Symbol used as Kafka key → same partition |

---

## 🧪 Try These Experiments

### 1. Event Replay
```bash
# Stop portfolio service (Ctrl+C)
# Delete consumer group offset:
docker exec -it kafka kafka-consumer-groups --bootstrap-server kafka:29092 \
  --group portfolio-service --delete

# Restart portfolio service → rebuilds state from scratch
cd portfolio-service && mvn exec:java
```

### 2. Inspect a Topic
```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic fills \
  --from-beginning \
  --max-messages 10
```

### 3. Check Consumer Lag
Open Kafka UI → Consumers → Pick any service → See lag per partition

---

## 📋 Current Status: Phase 1 Complete ✅

### ✅ Implemented
- All 6 services functional
- Event sourcing with full audit trail
- Idempotency in all consumers
- Real-time monitoring dashboard
- Kafka UI for inspection
- Replayability (can rebuild state from events)

### 🚧 Phase 2 Roadmap
- [ ] Fix ExecutionService race condition
- [ ] Add database persistence (PostgreSQL)
- [ ] Implement partial fills
- [ ] Add cash balance validation to risk
- [ ] Add Prometheus metrics
- [ ] Dead Letter Queue for error handling
- [ ] Unit & integration tests

### 🔮 Phase 3 Roadmap
- [ ] LIMIT orders (not just MARKET)
- [ ] Multi-partition scaling demo
- [ ] REST API for order submission
- [ ] Distributed tracing (OpenTelemetry)
- [ ] Multiple portfolio accounts

---

## 🛠️ Troubleshooting

**Services won't start?**
- Ensure Docker is running: `docker ps`
- Kafka ready? Check: `docker logs kafka`
- Port 9092 free? `lsof -i :9092`

**No events flowing?**
- Check Kafka UI → Topics → See message count
- Check service logs for errors
- Verify all services started in order

**Consumer lag growing?**
- Normal if service is slow
- Check service logs for processing errors
- Restart slow service if needed

---

## 📚 Tech Stack

- **Java 21** (modern Java features)
- **Apache Kafka 7.6.0** (event streaming)
- **Maven** (build tool)
- **Jackson** (JSON serialization)
- **SLF4J + Logback** (logging)
- **Docker Compose** (local infrastructure)

---

## 🤝 Contributing

This is a learning project! Feel free to:
- Add new services (alerts, reconciliation, analytics)
- Improve risk rules
- Add visualizations
- Write tests
- Optimize performance

---

## 📄 License

MIT License — free to use for learning and experimentation.

---

**Built with ❤️ to learn Kafka through real-world event-driven architecture**
