# 🧩 Data Generator

Realistic e-commerce event generator. Simulates orders, payments, and shipments flowing into Kafka topics.

## 📁 Structure

```
data-generator/
├── main.py                  # Entry point - orchestration
├── config.py               # Configuration (5 topics, rates, etc)
├── test_local.py           # Local test script (no Kafka needed)
│
├── domain/
│   ├── enums.py            # Constants (PAYMENT_STATUS, CARRIERS, etc)
│   └── policies.py         # Business rules (FaultPolicy)
│
├── ports/
│   └── event_publisher.py  # EventPublisher interface
│
├── adapters/
│   ├── kafka/
│   │   ├── factory.py      # Build Kafka producer
│   │   ├── publisher.py    # Implement EventPublisher
│   │   └── topics.py       # Create/delete topics
│   └── postgres/
│       └── seed.py         # Seed reference data
│
├── services/
│   ├── common.py           # Utilities (HotCache, IDs, etc)
│   └── orders.py           # Generate order → payment → shipment
│
└── util/
    └── rate_limit.py       # Token bucket + diurnal multiplier
```

## 🚀 Quick Start

### Local Test (No Docker)

```bash
cd infra/data-generator
python test_local.py
```

**Expected Output:**
```
📤 [orders.v1] ...
📤 [payments.v1] ...
📊 Summary: 5 events generated
```

### With Docker

```bash
# Build image
docker build -t ecommerce-datagen .

# Run with Kafka and PostgreSQL
docker run -d \
  -e KAFKA_BOOTSTRAP=kafka:9092 \
  -e PG_DSN="host=postgres port=5432 dbname=demo user=admin password=admin" \
  --network=host \
  ecommerce-datagen
```

## 🎯 Configuration

Edit `.env` or pass environment variables:

```bash
# Kafka
KAFKA_BOOTSTRAP=kafka:9092
TOPIC_ORDERS=orders.v1
TOPIC_PAYMENTS=payments.v1
TOPIC_SHIPMENTS=shipments.v1
CDC_TOPIC_USERS=demo.public.users
CDC_TOPIC_PRODUCTS=demo.public.products

# PostgreSQL (Seed reference data)
PG_DSN=host=postgres port=5432 dbname=demo user=admin password=admin

# Event rates
TARGET_EPS=120          # 120 events/second

# Seeds
SEED_USERS=500
SEED_PRODUCTS=200

# Probabilities
P_ORDER_HAS_PAYMENT=0.7
P_ORDER_HAS_SHIPMENT=0.6
P_BAD_RECORD=0.01       # 1% corrupted records
```

## 📊 Event Schema

### orders.v1
```json
{
  "order_id": "ord_xyz",
  "user_id": "usr_abc", 
  "product_id": "prd_123",
  "amount": 245.50,
  "currency": "USD",
  "ts": 1711843200000
}
```

### payments.v1
```json
{
  "payment_id": "pay_xyz",
  "order_id": "ord_xyz",
  "method": "CARD|APPLE_PAY|PAYPAL",
  "status": "PENDING|SETTLED|FAILED",
  "ts": 1711843200542
}
```

### shipments.v1
```json
{
  "shipment_id": "shp_xyz",
  "order_id": "ord_xyz",
  "carrier": "UPS|DHL|FEDEX",
  "eta_days": 3,
  "ts": 1711843215200
}
```

## 🔧 Probabilities

Each `emit()` call generates:

1. **Order** (100%) → `orders.v1`
2. **Payment** (70%) → `payments.v1`
   - PENDING: 25%
   - SETTLED: 62.5% ← Most common
   - FAILED: 12.5%
3. **Shipment** (60% if SETTLED) → `shipments.v1`

Example outcomes:
- Order A: Order → PENDING payment → No shipment ❌
- Order B: Order → SETTLED payment → Shipment ✅
- Order C: Order only → No payment (30% no payment) ❌

## 💡 Hexagonal Architecture

```
┌─────────────────────────────┐
│   services/                 │
│   (Order/Payment/Shipment)  │
└──────────┬──────────────────┘
           │ depends on
    ┌──────▼────────────┐
    │   ports/          │
    │   (Interfaces)    │
    └──────┬────────────┘
           │ implemented by
    ┌──────▼────────────┐
    │  adapters/        │
    │  (Kafka/Postgres) │
    └───────────────────┘
```

Benefits:
- Services don't know about Kafka/Postgres
- Easy to mock for testing
- Easy to swap adapters

## 📈 Performance

- **Memory:** ~50MB baseline
- **CPU:** Light (token bucket rate limiting)
- **Network:** Burst capable Kafka producer
- **Latency:** <1ms between order → payment → shipment

## 🧪 Testing

```bash
# Local test (no Kafka needed)
python test_local.py

# Docker test (with Kafka)
docker-compose up -d
docker exec -it datagen python test_local.py

# View Kafka topics (if running)
kcat -b kafka:9092 -L
```

## 📝 Notes

- Reference data (users, products) stored in PostgreSQL
- CDC (Debezium) captures updates → `demo.public.*` topics
- Streaming events are mock data from `OrderService`
- Rate limiting respects hourly patterns (diurnal curve)
- 1% of records corrupted for data quality testing
