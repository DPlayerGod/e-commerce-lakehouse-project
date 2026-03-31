"""Avro Schemas for E-commerce Events."""

ORDERS_SCHEMA = {
    "type": "record",
    "name": "Order",
    "namespace": "com.ecommerce.orders",
    "fields": [
        {"name": "order_id", "type": "string"},
        {"name": "user_id", "type": "string"},
        {"name": "product_id", "type": "string"},
        {"name": "amount", "type": "double"},
        {"name": "currency", "type": "string"},
        {"name": "ts", "type": "long"},
    ],
}

PAYMENTS_SCHEMA = {
    "type": "record",
    "name": "Payment",
    "namespace": "com.ecommerce.payments",
    "fields": [
        {"name": "payment_id", "type": "string"},
        {"name": "order_id", "type": "string"},
        {"name": "method", "type": "string"},
        {"name": "status", "type": "string"},
        {"name": "ts", "type": "long"},
    ],
}

SHIPMENTS_SCHEMA = {
    "type": "record",
    "name": "Shipment",
    "namespace": "com.ecommerce.shipments",
    "fields": [
        {"name": "shipment_id", "type": "string"},
        {"name": "order_id", "type": "string"},
        {"name": "carrier", "type": "string"},
        {"name": "eta_days", "type": "int"},
        {"name": "ts", "type": "long"},
    ],
}

DELIVERIES_SCHEMA = {
    "type": "record",
    "name": "Delivery",
    "namespace": "com.ecommerce.deliveries",
    "fields": [
        {"name": "delivery_id", "type": "string"},
        {"name": "shipment_id", "type": "string"},
        {"name": "order_id", "type": "string"},
        {"name": "status", "type": "string"},
        {"name": "reason", "type": "string"},
        {"name": "ts", "type": "long"},
    ],
}
