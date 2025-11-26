import json
import random
from datetime import datetime, timedelta
import uuid

CATEGORIES = ["Beauty", "Electronics", "Fashion", "Grocery", "Home"]

data = []
start = datetime.now()

for i in range(200):  # small but realistic dataset
    entry = {
        "order_id": str(uuid.uuid4()),
        "user_id": random.randint(1, 50),
        "amount": round(random.uniform(5, 300), 2),
        "category": random.choice(CATEGORIES),
        "timestamp": (start - timedelta(minutes=i)).isoformat()
    }
    data.append(entry)

with open("data/raw_orders.json", "w") as f:
    for d in data:
        f.write(json.dumps(d) + "\n")

print("Generated raw_orders.json")
