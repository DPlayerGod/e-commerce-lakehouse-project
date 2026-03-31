# Debezium for Ecommerce

This module runs Kafka Connect with Debezium and auto-provisions connectors from `config/*.json`.

## Included connector

- `demo-postgres.json`: captures `public.users` and `public.products` from Postgres `demo`.

## Runtime details

- Connect REST API: `http://localhost:8083`
- Topic prefix: `demo`
- Output topics: `demo.public.users`, `demo.public.products`

## Verify

```bash
curl http://localhost:8083/connectors
curl http://localhost:8083/connectors/demo-postgres/status
```
