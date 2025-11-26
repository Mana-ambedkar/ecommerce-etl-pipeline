import duckdb

con = duckdb.connect()

print("Top categories:")
print(con.execute("""
    SELECT category, SUM(amount) AS revenue
    FROM parquet_scan('data/clean_orders.parquet')
    GROUP BY category
    ORDER BY revenue DESC;
""").df())

print("\nDaily revenue:")
print(con.execute("""
    SELECT order_date, SUM(amount) AS daily_revenue
    FROM parquet_scan('data/clean_orders.parquet')
    GROUP BY order_date
    ORDER BY order_date;
""").df())
