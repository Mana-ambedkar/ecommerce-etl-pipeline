import duckdb
import os

# === ADD: Check if parquet file exists ===
if not os.path.exists("data/clean_orders.parquet"):
    print("✗ Error: data/clean_orders.parquet not found. Run 02_run_etl.py first.")
    exit(1)

# === ADD: Use context manager to auto-close connection ===
try:
    with duckdb.connect() as con:
        print("=" * 50)
        print("TOP CATEGORIES BY REVENUE")
        print("=" * 50)
        
        # === ADD: Error handling for queries ===
        try:
            result = con.execute("""
                SELECT 
                    category, 
                    SUM(amount) AS revenue,
                    COUNT(*) AS order_count,
                    ROUND(AVG(amount), 2) AS avg_order_value
                FROM parquet_scan('data/clean_orders.parquet')
                GROUP BY category
                ORDER BY revenue DESC;
            """).df()
            print(result.to_string(index=False))
        except Exception as e:
            print(f"✗ Error running category query: {e}")
        
        print("\n" + "=" * 50)
        print("DAILY REVENUE TREND")
        print("=" * 50)
        
        try:
            result = con.execute("""
                SELECT 
                    order_date, 
                    SUM(amount) AS daily_revenue,
                    COUNT(*) AS orders
                FROM parquet_scan('data/clean_orders.parquet')
                GROUP BY order_date
                ORDER BY order_date;
            """).df()
            print(result.to_string(index=False))
        except Exception as e:
            print(f"✗ Error running daily revenue query: {e}")
        
        # === ADD: Summary statistics ===
        print("\n" + "=" * 50)
        print("OVERALL SUMMARY")
        print("=" * 50)
        
        try:
            summary = con.execute("""
                SELECT 
                    COUNT(*) AS total_orders,
                    COUNT(DISTINCT user_id) AS unique_users,
                    ROUND(SUM(amount), 2) AS total_revenue,
                    ROUND(AVG(amount), 2) AS avg_order_value,
                    ROUND(MIN(amount), 2) AS min_order,
                    ROUND(MAX(amount), 2) AS max_order
                FROM parquet_scan('data/clean_orders.parquet');
            """).df()
            print(summary.to_string(index=False))
        except Exception as e:
            print(f"✗ Error running summary query: {e}")

except Exception as e:
    print(f"✗ Error connecting to DuckDB: {e}")
    exit(1)

print("\n✓ Analysis complete!")
