# Mini E-Commerce ETL Pipeline (Data Engineering Project)

This project simulates a real E-commerce data engineering workflow. 
It includes data ingestion, transformation, modeling, and SQL analytics — designed to showcase Big Data engineering skills for roles like TikTok’s Data Platform - E-Commerce Team.

## 🚀 Features
- Generates mock e-commerce order events (simulating Kafka ingestion)
- ETL pipeline using Spark-like transformations (pandas)
- Writes clean data into Parquet (common big-data format)
- Data modeling: FactOrders table
- SQL analytics using DuckDB
- End-to-end pipeline: Extract → Transform → Load → Query

## 📁 Project Structure
data/
│── raw_orders.json # raw “Kafka-like” events
│── clean_orders.parquet # analytics-ready fact table
generate_orders.py # ingestion simulation
etl_pipeline.py # ETL pipeline (clean + transform + load)
analytics.py # SQL analytics queries
README.md # documentation


## 🧩 Tech Used
- Python
- Pandas (Spark-style ETL)
- Parquet (columnar storage)
- DuckDB (SQL engine)
- Data modeling (fact table)
- Basic analytics queries

## 📊 Example SQL Output
- Category-level revenue
- Daily revenue trends
- User spend analysis

## 🎯 Why This Project?
This project highlights:
- ETL pipeline design
- Data ingestion simulation
- Data transformation logic
- Schema design + modeling
- SQL analytics
- E-commerce domain understanding

Perfect for Data Engineering internship applications.
