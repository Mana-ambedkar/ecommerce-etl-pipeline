import json
import pandas as pd
import duckdb
from datetime import datetime
import os
import sys

def extract_data(input_file):
    """Extract data from JSONL file with error handling."""
    print("=" * 60)
    print("EXTRACT PHASE")
    print("=" * 60)
    
    # Check if file exists
    if not os.path.exists(input_file):
        print(f"✗ Error: {input_file} not found")
        sys.exit(1)
    
    rows = []
    errors = 0
    
    try:
        with open(input_file) as f:
            for line_num, line in enumerate(f, 1):
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError as e:
                    errors += 1
                    print(f"⚠ Warning: Line {line_num} - Invalid JSON: {e}")
    except IOError as e:
        print(f"✗ Error reading file: {e}")
        sys.exit(1)
    
    if not rows:
        print("✗ Error: No valid data found")
        sys.exit(1)
    
    print(f"✓ Extracted {len(rows)} records")
    if errors > 0:
        print(f"⚠ Skipped {errors} malformed records")
    
    return pd.DataFrame(rows)

def validate_schema(df):
    """Validate required columns exist."""
    required_cols = ["order_id", "user_id", "category", "amount", "timestamp"]
    missing = set(required_cols) - set(df.columns)
    
    if missing:
        print(f"✗ Error: Missing required columns: {missing}")
        sys.exit(1)
    
    return True

def transform_data(df):
    """Transform and clean the data."""
    print("\n" + "=" * 60)
    print("TRANSFORM PHASE")
    print("=" * 60)
    
    initial_count = len(df)
    
    # Validate schema
    validate_schema(df)
    
    # Convert timestamp with error handling
    df["ts_clean"] = pd.to_datetime(df["timestamp"], errors='coerce')
    
    # Check for invalid timestamps
    invalid_ts = df["ts_clean"].isna().sum()
    if invalid_ts > 0:
        print(f"⚠ Removing {invalid_ts} records with invalid timestamps")
        df = df.dropna(subset=["ts_clean"])
    
    # Add date column (string type for better Parquet compatibility)
    df["order_date"] = df["ts_clean"].dt.date.astype(str)
    
    # Data quality checks
    
    # 1. Remove negative amounts
    negative_amounts = (df["amount"] < 0).sum()
    if negative_amounts > 0:
        print(f"⚠ Removing {negative_amounts} records with negative amounts")
        df = df[df["amount"] >= 0]
    
    # 2. Remove null amounts
    null_amounts = df["amount"].isna().sum()
    if null_amounts > 0:
        print(f"⚠ Removing {null_amounts} records with null amounts")
        df = df.dropna(subset=["amount"])
    
    # 3. Remove duplicate order_ids
    duplicates = df["order_id"].duplicated().sum()
    if duplicates > 0:
        print(f"⚠ Removing {duplicates} duplicate order_ids (keeping first)")
        df = df.drop_duplicates(subset=["order_id"], keep="first")
    
    # 4. Ensure valid categories
    df["category"] = df["category"].fillna("Unknown")
    
    # Clean schema - select only needed columns
    df = df[["order_id", "user_id", "category", "amount", "ts_clean", "order_date"]]
    
    final_count = len(df)
    removed = initial_count - final_count
    
    print(f"✓ Transformed {final_count} valid records")
    if removed > 0:
        print(f"  ({removed} records removed due to quality issues)")
    
    return df

def load_data(df, output_file):
    """Load data to Parquet file."""
    print("\n" + "=" * 60)
    print("LOAD PHASE")
    print("=" * 60)
    
    try:
        # Ensure directory exists
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        # Write to Parquet
        df.to_parquet(output_file, index=False)
        
        # Get file size
        file_size = os.path.getsize(output_file) / 1024  # KB
        
        print(f"✓ Loaded to {output_file}")
        print(f"  File size: {file_size:.2f} KB")
        
    except Exception as e:
        print(f"✗ Error writing Parquet file: {e}")
        sys.exit(1)

def run_analytics(df):
    """Run basic analytics on the transformed data."""
    print("\n" + "=" * 60)
    print("ANALYTICS PREVIEW")
    print("=" * 60)
    
    try:
        with duckdb.connect() as con:
            # Revenue by category
            result = con.execute("""
                SELECT 
                    category, 
                    SUM(amount) AS total_revenue,
                    COUNT(*) AS order_count,
                    ROUND(AVG(amount), 2) AS avg_order_value
                FROM df
                GROUP BY category
                ORDER BY total_revenue DESC;
            """).df()
            
            print("\nRevenue by Category:")
            print(result.to_string(index=False))
            
            # Summary stats
            summary = con.execute("""
                SELECT 
                    COUNT(*) AS total_orders,
                    COUNT(DISTINCT user_id) AS unique_users,
                    ROUND(SUM(amount), 2) AS total_revenue,
                    ROUND(AVG(amount), 2) AS avg_order
                FROM df;
            """).df()
            
            print("\nSummary Statistics:")
            print(summary.to_string(index=False))
            
    except Exception as e:
        print(f"⚠ Warning: Analytics failed: {e}")

def main():
    """Main ETL pipeline."""
    INPUT_FILE = "data/raw_orders.json"
    OUTPUT_FILE = "data/clean_orders.parquet"
    
    print("\n" + "=" * 60)
    print("ETL PIPELINE STARTING")
    print("=" * 60)
    
    # Extract
    df = extract_data(INPUT_FILE)
    
    # Transform
    df = transform_data(df)
    
    # Load
    load_data(df, OUTPUT_FILE)
    
    # Analytics (optional preview)
    run_analytics(df)
    
    print("\n" + "=" * 60)
    print("✓ ETL PIPELINE COMPLETED SUCCESSFULLY")
    print("=" * 60)

if __name__ == "__main__":
    main()
