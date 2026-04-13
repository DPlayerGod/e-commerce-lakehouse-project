"""Avro schemas used by silver transform."""

from __future__ import annotations

AVRO_ORDERS = """
{
  "type": "record",
  "name": "Order",
  "fields": [
    {"name": "order_id", "type": "string"},
    {"name": "user_id", "type": "string"},
    {"name": "product_id", "type": "string"},
    {"name": "quantity", "type": "int"},
    {"name": "amount", "type": "double"},
    {"name": "currency", "type": "string"},
    {"name": "ts", "type": "long"}
  ]
}
"""

AVRO_PAYMENTS = """
{
  "type": "record",
  "name": "Payment",
  "fields": [
    {"name": "payment_id", "type": "string"},
    {"name": "order_id", "type": "string"},
    {"name": "method", "type": "string"},
    {"name": "status", "type": "string"},
    {"name": "ts", "type": "long"}
  ]
}
"""

AVRO_SHIPMENTS = """
{
  "type": "record",
  "name": "Shipment",
  "fields": [
    {"name": "shipment_id", "type": "string"},
    {"name": "order_id", "type": "string"},
    {"name": "carrier", "type": "string"},
    {"name": "eta_days", "type": "int"},
    {"name": "ts", "type": "long"}
  ]
}
"""

AVRO_DELIVERIES = """
{
  "type": "record",
  "name": "Delivery",
  "fields": [
    {"name": "delivery_id", "type": "string"},
    {"name": "shipment_id", "type": "string"},
    {"name": "order_id", "type": "string"},
    {"name": "status", "type": "string"},
    {"name": "reason", "type": "string"},
    {"name": "ts", "type": "long"}
  ]
}
"""

AVRO_USERS_ENV = """
{"type":"record","name":"Envelope","namespace":"demo.public.users","fields":[{"name":"before","type":["null",{"type":"record","name":"Value","fields":[{"name":"id","type":{"type":"int","connect.default":0},"default":0},{"name":"user_id","type":"string"},{"name":"email","type":["null","string"],"default":null},{"name":"country","type":["null","string"],"default":null},{"name":"created_at","type":[{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.MicroTimestamp"},"null"],"default":0},{"name":"updated_at","type":[{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.MicroTimestamp"},"null"],"default":0}],"connect.name":"demo.public.users.Value"}],"default":null},{"name":"after","type":["null","Value"],"default":null},{"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.postgresql","fields":[{"name":"version","type":"string"},{"name":"connector","type":"string"},{"name":"name","type":"string"},{"name":"ts_ms","type":"long"},{"name":"snapshot","type":[{"type":"string","connect.version":1,"connect.parameters":{"allowed":"true,last,false,incremental"},"connect.default":"false","connect.name":"io.debezium.data.Enum"},"null"],"default":"false"},{"name":"db","type":"string"},{"name":"sequence","type":["null","string"],"default":null},{"name":"ts_us","type":["null","long"],"default":null},{"name":"ts_ns","type":["null","long"],"default":null},{"name":"schema","type":"string"},{"name":"table","type":"string"},{"name":"txId","type":["null","long"],"default":null},{"name":"lsn","type":["null","long"],"default":null},{"name":"xmin","type":["null","long"],"default":null}],"connect.name":"io.debezium.connector.postgresql.Source"}},{"name":"transaction","type":["null",{"type":"record","name":"block","namespace":"event","fields":[{"name":"id","type":"string"},{"name":"total_order","type":"long"},{"name":"data_collection_order","type":"long"}],"connect.version":1,"connect.name":"event.block"}],"default":null},{"name":"op","type":"string"},{"name":"ts_ms","type":["null","long"],"default":null},{"name":"ts_us","type":["null","long"],"default":null},{"name":"ts_ns","type":["null","long"],"default":null}],"connect.version":2,"connect.name":"demo.public.users.Envelope"}
"""

AVRO_PRODUCTS_ENV = """
{"type":"record","name":"Envelope","namespace":"demo.public.products","fields":[{"name":"before","type":["null",{"type":"record","name":"Value","fields":[{"name":"id","type":{"type":"int","connect.default":0},"default":0},{"name":"product_id","type":"string"},{"name":"title","type":["null","string"],"default":null},{"name":"category","type":["null","string"],"default":null},{"name":"price","type":["null","double"],"default":null},{"name":"created_at","type":[{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.MicroTimestamp"},"null"],"default":0},{"name":"updated_at","type":[{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.MicroTimestamp"},"null"],"default":0}],"connect.name":"demo.public.products.Value"}],"default":null},{"name":"after","type":["null","Value"],"default":null},{"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.postgresql","fields":[{"name":"version","type":"string"},{"name":"connector","type":"string"},{"name":"name","type":"string"},{"name":"ts_ms","type":"long"},{"name":"snapshot","type":[{"type":"string","connect.version":1,"connect.parameters":{"allowed":"true,last,false,incremental"},"connect.default":"false","connect.name":"io.debezium.data.Enum"},"null"],"default":"false"},{"name":"db","type":"string"},{"name":"sequence","type":["null","string"],"default":null},{"name":"ts_us","type":["null","long"],"default":null},{"name":"ts_ns","type":["null","long"],"default":null},{"name":"schema","type":"string"},{"name":"table","type":"string"},{"name":"txId","type":["null","long"],"default":null},{"name":"lsn","type":["null","long"],"default":null},{"name":"xmin","type":["null","long"],"default":null}],"connect.name":"io.debezium.connector.postgresql.Source"}},{"name":"transaction","type":["null",{"type":"record","name":"block","namespace":"event","fields":[{"name":"id","type":"string"},{"name":"total_order","type":"long"},{"name":"data_collection_order","type":"long"}],"connect.version":1,"connect.name":"event.block"}],"default":null},{"name":"op","type":"string"},{"name":"ts_ms","type":["null","long"],"default":null},{"name":"ts_us","type":["null","long"],"default":null},{"name":"ts_ns","type":["null","long"],"default":null}],"connect.version":2,"connect.name":"demo.public.products.Envelope"}
"""
