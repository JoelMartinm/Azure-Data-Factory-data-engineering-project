# End-to-End Retail Sales Data Engineering Pipeline using Azure Data Factory

## Overview

This repository contains an end-to-end **Azure Data Factory (ADF) data engineering project** that implements a **Bronze–Silver–Gold Lakehouse architecture** for retail transactional data.

The project demonstrates how raw transactional data can be ingested, cleaned, standardized, enriched, and modeled into analytics-ready fact and dimension tables using **ADF Mapping Data Flows (Spark-based)**.

The primary focus of this project is on:
- Data quality enforcement
- Business rule implementation
- Dimensional modeling
- Production-style transformations and aggregations

---
## Architecture

<img width="1536" height="1024" alt="ChatGPT Image Feb 3, 2026, 10_55_18 PM" src="https://github.com/user-attachments/assets/20929990-5906-425b-8607-11fbd65d8bbb" />

Azure Blob Storage (CSV)  
→ Bronze Layer (Raw)  
→ Silver Layer (Cleaned & Standardized)  
→ Gold Layer (Fact & Dimension Tables)

### Technologies Used
- Azure Data Factory  
- ADF Mapping Data Flows (Apache Spark)  
- Azure Blob Storage / ADLS Gen2  
- Parquet file format  

---

## Dataset Description

The dataset represents retail transactional data and includes:
- Transaction identifiers
- Customer identifiers
- Product and item information
- Pricing and quantity
- Payment method and sales channel
- Transaction date
- Discount indicator

Each row represents a single retail transaction.

---

## Bronze Layer (Raw)
<img width="926" height="282" alt="Screenshot 2026-02-16 081222" src="https://github.com/user-attachments/assets/4ff758a2-5c53-4aee-ae7c-c82df69002f7" />

### Purpose
The Bronze layer stores the raw data exactly as received from the source system.

### Characteristics
- Source: CSV file in Azure Blob Storage  
- No transformations applied  
- Schema-on-read  
- Used for auditing, replay, and recovery  

---

## Silver Layer (Cleaned and Standardized)
<img width="1389" height="445" alt="Screenshot 2026-02-16 081018" src="https://github.com/user-attachments/assets/4867f210-2f31-4a70-b480-c4c387caf57e" />


### Data Quality Filtering
Invalid records are removed:
- transaction_id IS NOT NULL  
- quantity > 0  
- total_spent >= 0  

### Derived Columns and Business Logic
- payment_method = lower(trim(payment_method))  
- transaction_date_clean = toDate(transaction_date)  
- total_spent = price_per_unit * quantity  
- has_discount = iif(discount_applied == true, 1, 0)  
- sales_sk = hash(transaction_id)  

Item codes are parsed to extract item number and category.

---

## Gold Layer (Business-Ready Data Models)
<img width="971" height="592" alt="Screenshot 2026-02-16 081126" src="https://github.com/user-attachments/assets/98762449-451a-458f-ade9-8d05439f56f0" />



### Fact Table: fact_sales
- Grain: one row per transaction  
- Measures: total_spent, quantity, has_discount  
- Keys: sales_sk, foreign keys to dimensions  

### Dimension Table: dim_customer
Aggregations:
- transaction_count = count(transaction_id)  
- total_spent_lifetime = sum(total_spent)  
- first_purchase_date = min(transaction_date)  
- last_purchase_date = max(transaction_date)  

### Dimension Table: dim_product
Aggregations:
- units_sold = sum(quantity)  
- total_revenue = sum(total_spent)  
- transaction_count = count(transaction_id)  
- first_sold_date = min(transaction_date)  
- last_sold_date = max(transaction_date)  

---
## Pipeline
<img width="1110" height="414" alt="Screenshot 2026-02-16 080841" src="https://github.com/user-attachments/assets/f06d659c-8673-4f7c-bd94-1bc6224a11f2" />

## Data Modeling

<img width="1536" height="1024" alt="image" src="https://github.com/user-attachments/assets/e6e5c683-80a9-4b83-8f1e-38a504de00e8" />

Star schema design optimized for analytics and BI tools.

---

## Execution Steps

1. Upload raw CSV files to Azure Blob Storage  
2. Build Mapping Data Flow in Azure Data Factory  
3. Apply transformations: Source → Filter → Derived Column → Select → Aggregate → Sink  
4. Write Silver and Gold layers in Parquet format  
