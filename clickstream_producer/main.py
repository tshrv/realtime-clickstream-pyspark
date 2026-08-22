"""
Python producer that simulates realistic e-commerce clickstream traffic.
The producer deliberately injects late-arriving events and duplicates so you can observe how Spark handles them in later steps.
"""

import json
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer
from loguru import logger


def main():
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    EVENT_TYPES = ["page_view", "add_to_cart", "purchase"]
    PAGES = ["/home", "/products", "/product/1", "/product/2", "/cart", "/checkout"]
    sent_events = []

    logger.info("Starting clickstream producer... (Ctrl+C to stop)")
    idx = 0
    try:
        while True:
            event_id = f"{idx}_{uuid.uuid4()}"
            now = datetime.now(tz=timezone.utc)

            # 10% of events are late (30-90 seconds behind)
            if random.random() < 0.10:
                delay = random.randint(30, 90)
                event_id += f"_d{delay}s"
                timestamp = now - timedelta(seconds=delay)
                logger.info(f"DELAYED: {delay}s | {timestamp}")
            else:
                timestamp = now

            event = {
                "event_id": event_id,
                "user_id": f"user_{random.randint(1, 50)}",
                "event_type": random.choice(EVENT_TYPES),
                "timestamp": timestamp.isoformat(),
                "page": random.choice(PAGES),
            }
            logger.info(f"#{idx} EVENT: {event}")
            producer.send("clickstream_events", value=event)
            sent_events.append(event)

            # 5% chance to resend a previous event (duplicate)
            if sent_events and random.random() < 0.05:
                duplicate = random.choice(sent_events[-20:])
                logger.info(f"DUPLICATE EVENT: {duplicate}")
                producer.send("clickstream_events", value=duplicate)

            idx += 1
            time.sleep(0.1)  # Adjust the sleep time to control event generation rate 

    except KeyboardInterrupt:
        logger.info("Stopping clickstream producer...")
    finally:
        producer.flush()
        producer.close()
        logger.info("Producer stopped")


if __name__ == '__main__':
    main()