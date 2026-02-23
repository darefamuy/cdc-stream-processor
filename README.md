# CDC Stream Processor

Real-time stateful stream processing over Oracle XStream CDC events.

## Architecture

```
Oracle 21c XE (BANKDB)
  └─ XStream Outbound Server (XOUT)
       └─ Confluent XStream CDC Connector
            └─ Kafka Topic: XEPDB1.BANKDB.TRANSACTIONS
                 │
                 ├─▶ [Pipeline 1] Velocity Fraud Detection
                 │       └─▶ abbank.notifications.fraud-alerts
                 │
                 ├─▶ [Pipeline 2] High-Value Transaction Alert
                 │       └─▶ abbank.notifications.high-value-alerts
                 │
                 ├─▶ [Pipeline 3] Running Balance Reconciliation
                 │       └─▶ abbank.notifications.balance-updates
                 │
                 ├─▶ [Pipeline 4] Account Dormancy Detection
                 │       └─▶ abbank.notifications.dormancy-alerts
                 │
                 └─▶ [Pipeline 5] Daily Spend Aggregation
                         └─▶ abbank.notifications.daily-spend
```

## Five Processing Pipelines

### 1. Velocity Fraud Detection (Tumbling Window)
Detects transaction bursts — N or more completed debits within a configurable
time window per account. Uses Kafka Streams windowed aggregation.

**State**: `KTable` backed by RocksDB  
**Window**: Tumbling (default: 60s)  
**Threshold**: Configurable via `ABBANK_VELOCITY_MAX_TXN` (default: 5)  
**Alert severity**: CRITICAL

### 2. High-Value Transaction Alert (Stateless Filter)
Emits an immediate notification for any single transaction above a configurable
NGN threshold. Includes before/after balance context in the notification body.

**Threshold**: Configurable via `ABBANK_HIGH_VALUE_THRESHOLD_NGN` (default: ₦500,000)  
**Alert severity**: HIGH (debit) / MEDIUM (credit)

### 3. Running Balance Reconciliation (Persistent KV Store)
Maintains a per-account running balance in a persistent RocksDB store.
On every completed transaction, compares the CDC-reported `balance_after`
against the stored value. Discrepancies beyond ₦0.01 tolerance trigger a
HIGH-severity alert.

**State**: Persistent KeyValue store (survives restarts)  
**Alert severity**: LOW (normal update) / HIGH (discrepancy detected)

### 4. Account Dormancy Detection (Session Window)
Session windows close when no activity is observed for the inactivity gap
(default: 30 days). Accounts with only a single event in a session (i.e.
the opening transaction followed by silence) trigger a dormancy notification.

**Window**: Session with configurable inactivity gap  
**Alert channel**: EMAIL only  
**Alert severity**: LOW

### 5. Daily Spend Aggregation (Tumbling Daily Window)
Aggregates debit totals per account in 24-hour tumbling windows.
Emits an SMS alert when the cumulative daily debit exceeds the threshold.

**Window**: Tumbling 24h  
**Threshold**: Configurable via `ABBANK_DAILY_SPEND_ALERT_NGN` (default: ₦1,000,000)  
**Alert channel**: SMS  
**Alert severity**: MEDIUM

## Building

```bash
# Build fat JAR
./gradlew shadowJar

# Run tests with coverage
./gradlew test jacocoTestReport

# Build Docker image
docker build -t cdc-stream-processor:latest .
```

## Running

### Standalone (local Kafka)
```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export SCHEMA_REGISTRY_URL=http://localhost:8081
java -jar build/libs/cdc-stream-processor.jar
```

### Docker (integrated with AB Bank demo stack)
```bash
# 1. Copy this directory into the demo project
# 2. Merge the service block from docker-compose-service.yml into the main compose file
# 3. Add  streams-state:  to the volumes section

# 4. Start
cd ../oracle-xstream-cdc-connector-e2e-demo
docker compose up -d cdc-stream-processor
```

## Health Checks

| Endpoint            | Use                                   | Success |
|---------------------|---------------------------------------|---------|
| `GET /health`       | Docker / K8s **liveness** probe       | 200 OK  |
| `GET /ready`        | Docker / K8s **readiness** probe      | 200 OK  |
| `GET /metrics`      | Application state summary             | 200 OK  |

```bash
curl http://localhost:8080/health
# {"status":"RUNNING","probe":"liveness","timestamp":"..."}

curl http://localhost:8080/metrics
# {"state":"RUNNING","application":"cdc-stream-processor","version":"1.0.0","timestamp":"..."}
```

## Configuration Reference

All settings are in `src/main/resources/application.conf` and can be
overridden via environment variables:

| Env Variable                    | Default    | Description                          |
|---------------------------------|------------|--------------------------------------|
| `KAFKA_BOOTSTRAP_SERVERS`       | `localhost:9092` | Kafka broker(s)              |
| `SCHEMA_REGISTRY_URL`           | `http://localhost:8081` | Schema Registry URL   |
| `STREAMS_APPLICATION_ID`        | `cdc-stream-processor-v1` | Consumer group / app ID  |
| `STREAMS_NUM_THREADS`           | `4`        | Stream processing threads            |
| `ABBANK_HIGH_VALUE_THRESHOLD_NGN` | `500000` | High-value alert threshold (₦)     |
| `ABBANK_VELOCITY_MAX_TXN`       | `5`        | Max txns before velocity fraud alert |
| `ABBANK_VELOCITY_WINDOW_SEC`    | `60`       | Velocity detection window (seconds)  |
| `ABBANK_DORMANCY_DAYS`          | `30`       | Session inactivity gap (days)        |
| `ABBANK_DAILY_SPEND_ALERT_NGN`  | `1000000`  | Daily spend alert threshold (₦)     |

## Output Topics

| Topic                                  | Type         | Consumer              |
|----------------------------------------|--------------|-----------------------|
| `abbank.notifications.fraud-alerts`   | JSON string  | Notification Gateway  |
| `abbank.notifications.high-value-alerts` | JSON string | Notification Gateway |
| `abbank.notifications.balance-updates`| JSON string  | Notification Gateway  |
| `abbank.notifications.dormancy-alerts`| JSON string  | Notification Gateway  |
| `abbank.notifications.daily-spend`    | JSON string  | Notification Gateway  |

## Project Structure

```
cdc-stream-processor/
├── build.gradle
├── settings.gradle
├── gradle.properties
├── Dockerfile
├── docker-compose-service.yml
├── src/
│   ├── main/
│   │   ├── avro/                                  ← Avro schemas
│   │   ├── java/com/abbank/streams/
│   │   │   ├── ABBankStreamsApp.java              ← entry point
│   │   │   ├── config/
│   │   │   │   └── ABBankStreamsConfig.java       ← all config
│   │   │   ├── topology/
│   │   │   │   ├── ABBankTopology.java            ← 5 processing pipelines
│   │   │   │   └── BalanceReconciliationProcessor.java
│   │   │   ├── model/
│   │   │   │   ├── CdcEnvelope.java
│   │   │   │   ├── TransactionEvent.java
│   │   │   │   ├── NotificationEvent.java
│   │   │   │   ├── AccountEvent.java
│   │   │   │   ├── CustomerEvent.java
│   │   │   │   ├── VelocityState.java
│   │   │   │   └── DailySpendState.java
│   │   │   ├── serde/
│   │   │   │   ├── JsonSerde.java
│   │   │   │   └── AvroSerdes.java
│   │   │   ├── health/HealthServer.java
│   │   │   └── util/CdcParser.java
│   │   └── resources/
│   │       ├── application.conf
│   │       └── logback.xml
│   └── test/
│       └── java/com/abbank/streams/
│           └── topology/
│               └── ABBankTopologyTest.java
└── README.md
```
