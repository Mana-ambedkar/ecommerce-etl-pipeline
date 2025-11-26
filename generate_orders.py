import json
import random
import os
from datetime import datetime, timedelta
import uuid


NUM_ORDERS = 200
NUM_USERS = 50
MIN_AMOUNT = 5.0
MAX_AMOUNT = 300.0
CATEGORIES = ["Beauty", "Electronics", "Fashion", "Grocery", "Home"]


os.makedirs("data", exist_ok=True)

data = []
start = datetime.now()

for i in range(NUM_ORDERS):
    entry = {
        "order_id": str(uuid.uuid4()),
        "user_id": random.randint(1, NUM_USERS),
        "amount": round(random.uniform(MIN_AMOUNT, MAX_AMOUNT), 2),
        "category": random.choice(CATEGORIES),
        "timestamp": (start - timedelta(minutes=i)).isoformat()
    }
    data.append(entry)

try:
    with open("data/raw_orders.json", "w") as f:
        for d in data:
            f.write(json.dumps(d) + "\n")
    print(f"✓ Generated {NUM_ORDERS} orders → data/raw_orders.json")
except IOError as e:
    print(f"✗ Error writing file: {e}")
    exit(1)
