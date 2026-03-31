# 🧩 Data Generator

Realistic e-commerce event generator. Simulates orders, payments, and shipments flowing into Kafka topics.

## 📁 Structure

```
data-generator/
├── main.py                  # Entry point - orchestration
├── config.py               # Configuration (6 topics, rates, etc)
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
TOPIC_DELIVERIES=delivery-status.v1
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

### Example Transaction Flow

**Transaction #1** (No shipment - payment failed)
```
📤 [orders.v1] ord_nijh5rubzg
{
  "order_id": "ord_nijh5rubzg",
  "user_id": "usr_fp5awmv9",
  "product_id": "prd_lslfir0r",
  "amount": 181.62,
  "currency": "EUR",
  "ts": 1774940477412
}

📤 [payments.v1] pay_bayfgfo7xk
{
  "payment_id": "pay_bayfgfo7xk",
  "order_id": "ord_nijh5rubzg",
  "method": "APPLE_PAY",
  "status": "FAILED",
  "ts": 1774940478466
}
```

**Transaction #2** (Pending payment - no shipment)
```
📤 [orders.v1] ord_wnaidadiiw
{
  "order_id": "ord_wnaidadiiw",
  "user_id": "usr_lh20fkgh",
  "product_id": "prd_lslfir0r",
  "amount": 628.44,
  "currency": "GBP",
  "ts": 1774940477412
}

📤 [payments.v1] pay_xafjp61kmk
{
  "payment_id": "pay_xafjp61kmk",
  "order_id": "ord_wnaidadiiw",
  "method": "CARD",
  "status": "PENDING",
  "ts": 1774940478514
}
```

**Transaction #3** (Full flow: Order → Payment → Shipment → Delivery)
```
📤 [orders.v1] ord_tmy0330ysu
{
  "order_id": "ord_tmy0330ysu",
  "user_id": "usr_y4ezcabl",
  "product_id": "prd_lslfir0r",
  "amount": 835.58,
  "currency": "USD",
  "ts": 1774940477420
}

📤 [payments.v1] pay_htvaiwf23y
{
  "payment_id": "pay_htvaiwf23y",
  "order_id": "ord_tmy0330ysu",
  "method": "CARD",
  "status": "SETTLED",
  "ts": 1774940477447
}

📤 [shipments.v1] shp_q9p2808tgt
{
  "shipment_id": "shp_q9p2808tgt",
  "order_id": "ord_tmy0330ysu",
  "carrier": "FEDEX",
  "eta_days": 2,
  "ts": 1774940489190
}

📤 [delivery-status.v1] del_97ce0otx0x
{
  "delivery_id": "del_97ce0otx0x",
  "shipment_id": "shp_q9p2808tgt",
  "order_id": "ord_tmy0330ysu",
  "status": "DELIVERED",
  "reason": "left_at_door",
  "ts": 1775148559772
}
```

## 🔧 Probabilities

Each `emit()` call generates:

1. **Order** (100%) → `orders.v1`
2. **Payment** (70%) → `payments.v1`
3. **Shipment** (60% if SETTLED) → `shipments.v1`
4. **Delivery Status** (100% if Shipment) → `delivery-status.v1`
   - DELIVERED: 80% ← Most common (reasons: `customer_home`, `signed`, `left_at_door`)
   - FAILED: 15% (reasons: `customer_not_home`, `address_unclear`, `weather_delay`, `vehicle_issue`)
   - RETURNED: 5% (reasons: `customer_refused`, `damaged_in_transit`)

Example outcomes:
- Order A: Order → PENDING payment → No shipment ❌
- Order B: Order → SETTLED payment → Shipment → Delivery Status ✅
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
- Rate limiting respects hourly patterns: peak traffic 18h-22h (6 PM-10 PM), lowest traffic around 3 AM
- 1% of records corrupted for data quality testing
