import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

# Possible user attributes
SEGMENTS = ["premium", "standard", "new_user", "churning"]
NAMES = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Hank"]
# Defines four user segments and eight sample names that will be randomly assigned to profiles.


def main():
    # Connect to local Kafka broker
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    print("Starting profile producer... (Ctrl+C to stop)")

    try:
        while True:
            user_idx = random.randint(1, 8) # both inclusive
            user_id = f"user_{user_idx}"
            # Build a profile update event
            profile = {
                "user_id": user_id,
                "name": NAMES[user_idx - 1],  # Adjust index for 0-based list
                "segment": random.choice(SEGMENTS),
                "profile_timestamp": datetime.now(tz=timezone.utc).isoformat(),
            }
            print(f"PROFILE UPDATE: {profile}")
            producer.send("user_profiles", value=profile)
            time.sleep(2)
    except KeyboardInterrupt:
        print("Profile producer stopped.")
    finally:
        producer.flush()
        producer.close()


if __name__ == '__main__':
    main()