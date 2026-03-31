"""Kafka Topics - Create and delete demo topics."""
from time import sleep, monotonic
from confluent_kafka.admin import AdminClient, NewTopic

from config import Config


def clear_kafka(config: Config) -> None:
    """Delete and recreate all Kafka topics."""
    topics = [
        config.topic_orders,
        config.topic_payments,
        config.topic_shipments,
        config.topic_deliveries,
        config.cdc_topic_users,
        config.cdc_topic_products,
    ]
    topics = list(dict.fromkeys(topics))
    
    admin = AdminClient({"bootstrap.servers": config.bootstrap})
    
    print("🧹 Deleting Kafka topics...")
    futures = admin.delete_topics(topics, operation_timeout=10)
    for t, f in futures.items():
        try:
            f.result()
            print(f"  ✨ Deleted topic {t}")
        except Exception as e:
            print(f"  ⚠️ Delete failed for {t}: {e}")
    
    # Wait for topics to be fully deleted before recreating
    sleep(5)
    
    print("📝 Re-creating topics...")
    new_topics = [NewTopic(t, num_partitions=3, replication_factor=1) for t in topics]
    futures = admin.create_topics(new_topics)
    for t, f in futures.items():
        try:
            f.result()
            print(f"  ✅ Re-created topic {t}")
        except Exception as e:
            print(f"  ⚠️ Create failed for {t}: {e}")
    
    # Wait for topics to be fully created and ready for publishing
    start = monotonic()
    while monotonic() - start < 15:  # Wait up to 15 seconds
        try:
            md = admin.list_topics(timeout=5)
            available_topics = set(md.topics.keys())
            if all(t in available_topics for t in topics):
                print(f"  ✓ All topics are ready for publishing")
                return
        except Exception:
            pass
        sleep(1)
    
    print("  ⚠️ Topics may not be fully ready, proceeding anyway...")
