"""Main entry point - Data Generator orchestration."""
from __future__ import annotations

import signal
from time import sleep, monotonic

import psycopg2
from confluent_kafka.schema_registry import SchemaRegistryClient

from config import Config
from adapters.kafka.factory import build_kafka
from adapters.kafka.topics import clear_kafka
from adapters.postgres.seed import seed_postgres
from adapters.postgres.maintenance import DbMaintenanceAdapter
from services.common import HotCache
from services.orders import OrderService
from services.deliveries import DeliveryService
from util.rate_limit import TokenBucket


def wait_for_pg(dsn: str, max_wait_s: int = 60) -> None:
    """Wait for PostgreSQL to be ready."""
    start = monotonic()
    while True:
        try:
            conn = psycopg2.connect(dsn)
            conn.close()
            print("[fakegen] ✅ PostgreSQL is ready")
            return
        except Exception:
            if monotonic() - start > max_wait_s:
                raise
            print("[fakegen] ⏳ Waiting for PostgreSQL...")
            sleep(1)


def wait_for_sr(url: str, max_wait_s: int = 60) -> None:
    """Wait for Schema Registry to be ready."""
    start = monotonic()
    client = SchemaRegistryClient({"url": url})
    while True:
        try:
            client.get_subjects()
            print("[fakegen] ✅ Schema Registry is ready")
            return
        except Exception:
            if monotonic() - start > max_wait_s:
                raise
            print("[fakegen] ⏳ Waiting for Schema Registry...")
            sleep(1)


class App:
    """Main application."""

    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.running = True
        self.cache = HotCache()

    def _setup(self):
        """Setup - Initialize connections and data."""
        print("[fakegen] waiting for PostgreSQL…", flush=True)
        wait_for_pg(self.cfg.pg_dsn)
        self.conn = psycopg2.connect(self.cfg.pg_dsn)

        print("[fakegen] Seeding PostgreSQL...")
        seed_postgres(self.conn, self.cfg, self.cache)

        print("[fakegen] waiting for Schema Registry…", flush=True)
        wait_for_sr(self.cfg.schema_registry)

        print("[fakegen] Building Kafka producer...")
        self.publisher, self.encoders = build_kafka(self.cfg)

        print("[fakegen] Clearing and recreating Kafka topics...")
        clear_kafka(self.cfg)

        print("[fakegen] Creating services...")
        self.order_service = OrderService(
            self.cfg,
            self.cache,
            self.publisher,
            self.encoders[self.cfg.topic_orders],
            self.encoders[self.cfg.topic_payments],
            self.encoders[self.cfg.topic_shipments],
        )
        self.delivery_service = DeliveryService(
            self.cfg,
            self.publisher,
            self.encoders[self.cfg.topic_deliveries],
        )
        self.maintenance = DbMaintenanceAdapter(
            self.conn,
            self.cfg,
            self.cache,
        )

    def _loop(self):
        """Main event loop."""
        bucket = TokenBucket(rate=self.cfg.target_eps)
        print(
            f"[fakegen] 🚀 Starting event generation - Target: {self.cfg.target_eps} EPS",
            flush=True,
        )

        while self.running:
            bucket.refill()

            while bucket.try_consume(1.0):
                shipment_data = self.order_service.emit()
                if shipment_data:
                    self.delivery_service.emit_delivery(
                        shipment_data["shipment_id"],
                        shipment_data["order_id"],
                        shipment_data["shipment_ts"],
                        shipment_data["eta_days"],
                    )

            # Randomly update DB records (trigger CDC events)
            self.maintenance.maybe_update_user_info()
            self.maintenance.maybe_update_product_price()

            self.publisher.poll()
            sleep(0.005)

    def _teardown(self):
        """Cleanup."""
        print("[fakegen] 🛑 Shutting down...", flush=True)
        try:
            self.publisher.flush(15)
        finally:
            self.conn.close()

    def run(self):
        """Run the application."""
        self._setup()
        try:
            self._loop()
        finally:
            self._teardown()


def main():
    cfg = Config()
    app = App(cfg)

    def _sig(sig, frame):
        app.running = False
        print("\n[fakegen] 🛑 Shutdown signal received")

    signal.signal(signal.SIGINT, _sig)
    signal.signal(signal.SIGTERM, _sig)

    try:
        app.run()
    except Exception as e:
        print(f"[fakegen] ❌ Error: {e}")
        raise


if __name__ == "__main__":
    main()
