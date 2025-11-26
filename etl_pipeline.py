import json
import pandas as pd
import duckdb
from datetime import datetime

# ====== EXTRACT ======
rows = []
with open("data/raw_orders.json") as f:
    for line in f:
        rows.append(json.loads(line))

df = pd.DataFrame(rows)

# ====== TRANSFORM ======
# Convert timestamp
df["ts_clean"] = pd.to_datetime(df["timestamp"])

# Add date column for analytics
df["order_date"] = df["ts_clean"].dt.date

# Clean schema
df = df[["order_id", "user_id", "category", "amount", "ts_clean", "order_date"]]

# ====== LOAD (Parquet Fact Table) ======
df.to_parquet("data/clean_orders.parquet")

print("ETL complete → clean_orders.parquet created")

# ====== OPTIONAL: RUN SQL WITH DUCKDB ======
con = duckdb.connect()

result = con.execute("""
    SELECT category, SUM(amount) AS total_revenue
    FROM df
    GROUP BY category
    ORDER BY total_revenue DESC;
""").df()

print("\nRevenue by Category:")
print(result)
