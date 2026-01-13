import json
import uuid
import random
import os
from datetime import datetime, timedelta

# Configuration
OUTPUT_DIR = "data/input_events"
NUM_TRANSACTIONS = 100
USER_IDS = [f"user_{i}" for i in range(1, 21)]  # 20 users
MERCHANT_IDS = ["amazon", "netflix", "apple", "unknown_shop", "crypto_exchange"]
COUNTRIES = ["UY", "AR", "BR", "US", "ES"]

def generate_transaction():
    """Generates a single synthetic payment event."""
    event_ts = datetime.now()
    
    # Simulate some "late data" (5% of events are 10 minutes old)
    if random.random() < 0.05:
        event_ts = event_ts - timedelta(minutes=10)

    return {
        "event_id": str(uuid.uuid4()),
        "event_ts": event_ts.isoformat(),
        "payment_id": str(uuid.uuid4()),
        "user_id": random.choice(USER_IDS),
        "merchant_id": random.choice(MERCHANT_IDS),
        "amount": round(random.uniform(5.0, 15000.0), 2), # High range for fraud rules
        "currency": "USD",
        "payment_method": random.choice(["card", "wallet", "bank_transfer"]),
        "ip_address": f"192.168.1.{random.randint(1, 254)}",
        "device_id": f"device_{random.randint(1, 50)}",
        "status": random.choice(["captured", "captured", "captured", "failed", "refunded"])
    }

def main():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    transactions = [generate_transaction() for _ in range(NUM_TRANSACTIONS)]
    
    # Save as a single JSON file for the batch demo
    filename = f"payments_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    filepath = os.path.join(OUTPUT_DIR, filename)
    
    with open(filepath, "w") as f:
        # We save it as JSON Lines (one JSON per line), which is standard for Spark
        for tx in transactions:
            f.write(json.dumps(tx) + "\n")
            
    print(f"Successfully generated {NUM_TRANSACTIONS} events in {filepath}")

if __name__ == "__main__":
    main()