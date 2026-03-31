# E-commerce Data Generator with Avro Encoding

This is a event streaming pipeline for an e-commerce lakehouse project, generating production-grade Avro-encoded events.

## Architecture Overview

**6 Event Topics:**
- `orders.v1` - Order creation events
- `payments.v1` - Payment processing events (70% of orders)
- `shipments.v1` - Shipment/delivery events (60% of payments)
- `delivery-status.v1` - Delivery status updates (100% of shipments, timed after ETA)
- `demo.public.users` - User CDC events (reference data updates)
- `demo.public.products` - Product CDC events (reference data updates)

**Serialization:** Avro with Schema Registry for schema evolution and compression

**Event Rate:** 120 events/second (configurable via `TARGET_EPS`)

## Quick Start

### 1. Start Infrastructure

```bash
cd E-commerce-project
docker-compose up -d
```

This starts:
- **PostgreSQL** (port 5432) - Reference data
- **Kafka** (port 9092) - Event broker
- **Schema Registry** (port 8081) - Schema storage
- **Debezium Connect** (port 8083) - CDC runtime for users/products
- **Kafka UI** (port 8080) - Web UI for debugging [optional]

Wait for services to be healthy:
```bash
docker-compose ps
```

### 2. Install Dependencies

```bash
cd infra/data-generator
pip install -r requirements.txt
```

### 3. Set Up Database

```bash
python -c "from adapters.postgres.seed import setup; setup()"
```

This creates:
- `demo` database
- `users` table (500 seed users)
- `products` table (200 seed products)
- `orders` table
- Event tables for auditing

### 4. Create Kafka Topics

```bash
# Create all required topics
./scripts/setup-topics.sh
```

Or manually:
```bash
docker-compose exec kafka kafka-topics \
  --create --topic orders.v1 --bootstrap-server localhost:9092 --if-not-exists
docker-compose exec kafka kafka-topics \
  --create --topic payments.v1 --bootstrap-server localhost:9092 --if-not-exists
# ... repeat for other topics
```

### 5. Verify Debezium Connector

Debezium starts from `infra/debezium` and auto-applies connector configs from:

- `infra/debezium/config/demo-postgres.json`

Check connector status:

```bash
curl http://localhost:8083/connectors/demo-postgres/status
```

Expected CDC topics:

- `demo.public.users`
- `demo.public.products`

### 6. Run Data Generator

```bash
python main.py
```

Output:
```
[app] Waiting for Schema Registry (http://schema-registry:8081)...
[app] Schema Registry ready ✓
[app] OrderService initialized with Avro encoders
[app] DeliveryService initialized with Avro encoder
[app] Starting e-commerce data generator...
[app] 📊 EPS: 120.0 | Orders: 125 | Payments: 87 | Shipments: 52 | Deliveries: 48 | CDC: 12
[app] 📊 EPS: 120.1 | Orders: 248 | Payments: 174 | Shipments: 104 | Deliveries: 96 | CDC: 21
```

## Configuration

Environment variables (see [data-generator/config.py](infra/data-generator/config.py)):

```bash
# Kafka
KAFKA_BOOTSTRAP=kafka:9092
SCHEMA_REGISTRY_URL=http://schema-registry:8081

# Event rates
TARGET_EPS=120

# Probabilities
P_ORDER_HAS_PAYMENT=0.7          # 70% of orders → payments
P_ORDER_HAS_SHIPMENT=0.6         # 60% of payments → shipments
P_SHIPMENT_HAS_DELIVERY=1.0      # 100% of shipments → deliveries

# Database updates
P_UPDATE_USER_INFO=0.08          # 8% of events trigger user updates
P_UPDATE_PRODUCT_PRICE=0.05      # 5% of events trigger price updates

# Late events (config only, not yet implemented)
P_LATE_EVENT=0.05
MAX_LATE_MINUTES=25

# Delivery timing
ETA_DAYS_MIN=1
ETA_DAYS_MAX=7                   # Delivery occurs 1-7 days after shipment
```

## Monitoring

### Kafka UI

Visit http://localhost:8080 in your browser to:
- View topics and partitions
- Inspect message payloads (Avro decoded)
- Monitor consumer groups
- Check Schema Registry subjects

### View Events

```bash
# Consume orders with key and value
docker-compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic orders.v1 \
  --from-beginning \
  --property print.key=true

# View all registered schemas
curl http://localhost:8081/subjects

# View schema for orders.v1-value
curl http://localhost:8081/subjects/orders.v1-value/versions/latest
```

### Check Database

```bash
docker-compose exec postgres psql -U admin -d demo -c "SELECT * FROM orders LIMIT 5;"
docker-compose exec postgres psql -U admin -d demo -c "SELECT * FROM users LIMIT 5;"
```

## Event Flow Timeline

1. **Order Created** → `orders.v1` (ts₀)
2. **Payment Processed** (70% chance) → `payments.v1` (ts₀ + offset)
3. **Shipment Created** (60% chance) → `shipments.v1` (ts₀ + offset, includes eta_days)
4. **Delivery Status** (100% chance) → `delivery-status.v1` (ts + eta_days * 86400000 ms + 0-12h random)
5. **DB Updates** → `demo.public.users` or `demo.public.products` (async, triggered randomly)

## Key Features

✅ **Avro Serialization** - Binary encoding with schema evolution  
✅ **Schema Registry** - Centralized schema management  
✅ **Realistic Timings** - Events respect business logic timestamps  
✅ **CDC Integration** - Database updates trigger Kafka events  
✅ **Token Bucket Rate Limiting** - Smooth event distribution with temporal variation  
✅ **Event Correlation** - Related events share IDs for tracing  
✅ **Hot Cache** - 95% reuse of user/product data  

## Troubleshooting

### Schema Registry Connection Failed
```
[avro] ⚠️ Schema registration failed: Failed to initialize client
```
**Solution:** Ensure Schema Registry is running and healthy
```bash
docker-compose logs schema-registry
curl http://localhost:8081/subjects
```

### Kafka Connection Failed
```
Failed to connect to Kafka broker
```
**Solution:** Check Kafka service
```bash
docker-compose logs kafka
docker-compose ps
```

### PostgreSQL Connection Failed
```
could not connect to server: Connection refused
```
**Solution:** Seed database and check connection
```bash
python -c "from adapters.postgres.seed import setup; setup()"
docker-compose exec postgres psql -U admin -d demo -c "SELECT 1;"
```

### Out of Memory
```
Killed DUE TO MEMORY LIMIT
```
**Solution:** Reduce TARGET_EPS or increase Docker memory limit
```bash
DOCKER_MEMORY=4g docker-compose up
```

## Architecture Details

### Hexagonal (Ports & Adapters) Design

```
main.py
  ├─ ports/
  │  ├─ kafka_publisher.py (abstract)
        └─ adapters/kafka/publisher.py (implementation)
  ├─ services/
  │  ├─ orders.py
  │  └─ deliveries.py
  ├─ adapters/
  │  ├─ kafka/ (encoder, factory, schemas, publisher)
  │  └─ postgres/ (seed, maintenance)
  └─ config.py
```

### Avro Encoding Pipeline

```
Data Dict
  ↓
AvroEncoder.encode()
  ├─ Create AvroSerializer with SchemaRegistryClient
  ├─ Register schema if not exists
  └─ Serialize dict to Avro bytes
  ↓
Publish to Kafka
  └─ Message: key (ID) + value (Avro bytes)
```

## Next Steps

- [ ] Integrate with Spark Bronze layer for ingestion
- [ ] Add Iceberg table format for storage
- [ ] Implement end-to-end schema evolution scenarios
- [ ] Add data quality checks (Great Expectations)
- [ ] Deploy to Kubernetes

## References

- [data-forge Architecture](../../../data-forge/docs/architecture.md)
- [Confluent Avro Documentation](https://docs.confluent.io/kafka-clients/python/current/overview.html)
- [Schema Registry REST API](https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html)
- [Zoomcamp26 Project Guide](../../README.md)

