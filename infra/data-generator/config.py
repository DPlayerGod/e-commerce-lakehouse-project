"""Configuration - Read from environment variables."""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    """Application configuration."""

    # Kafka
    bootstrap: str = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
    schema_registry: str = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    topic_orders: str = os.getenv("TOPIC_ORDERS", "orders.v1")
    topic_payments: str = os.getenv("TOPIC_PAYMENTS", "payments.v1")
    topic_shipments: str = os.getenv("TOPIC_SHIPMENTS", "shipments.v1")
    topic_deliveries: str = os.getenv("TOPIC_DELIVERIES", "delivery-status.v1")

    # CDC Topics
    cdc_topic_users: str = os.getenv("CDC_TOPIC_USERS", "demo.public.users")
    cdc_topic_products: str = os.getenv("CDC_TOPIC_PRODUCTS", "demo.public.products")

    # PostgreSQL
    pg_dsn: str = os.getenv(
        "PG_DSN", "host=postgres port=5432 dbname=demo user=admin password=admin"
    )

    # Event rates
    target_eps: float = float(os.getenv("TARGET_EPS", "120"))

    # Seeds
    seed_users: int = int(os.getenv("SEED_USERS", "500"))
    seed_products: int = int(os.getenv("SEED_PRODUCTS", "200"))

    # Probabilities
    p_order_has_payment: float = float(os.getenv("P_ORDER_HAS_PAYMENT", "0.7"))
    p_order_has_shipment: float = float(os.getenv("P_ORDER_HAS_SHIPMENT", "0.6"))
    p_bad_record: float = float(os.getenv("P_BAD_RECORD", "0.01"))

    # DB Updates (CDC triggers)
    p_update_user_info: float = float(os.getenv("P_UPDATE_USER_INFO", "0.08"))
    p_update_product_price: float = float(os.getenv("P_UPDATE_PRODUCT_PRICE", "0.05"))

    # Late events
    p_late_event: float = float(os.getenv("P_LATE_EVENT", "0.05"))
    max_late_minutes: int = int(os.getenv("MAX_LATE_MINUTES", "25"))

    # Delivery config
    p_shipment_has_delivery: float = float(os.getenv("P_SHIPMENT_HAS_DELIVERY", "1.0"))
    eta_days_min: int = int(os.getenv("ETA_DAYS_MIN", "1"))
    eta_days_max: int = int(os.getenv("ETA_DAYS_MAX", "7"))
