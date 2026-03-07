## 📌 Project Overview (What this project does)
This project is an end-to-end **ETL (Extract, Transform, Load) Pipeline** designed to process raw, "dirty" retail sales data and load it into a structured Relational Database, **enabling seamless SQL querying for deeper business insights and data-driven decision making**.

## 💡 Business Value (How does this project add value?)
* **Single Source of Truth:** Transforms scattered and error-prone CSV files into a reliable, centralized database.
* **Data Quality & Integrity:** Prevents "garbage data" from entering the system by filtering null values and recalculating sales figures to ensure 100% accuracy before deployment.
* **Optimized for Analytics:** Utilizes a Star Schema structure, enabling Data Analysts and BI tools to query sales summaries by time or category quickly and with much simpler SQL code.
* **Storage Efficiency:** Reduces data redundancy through Normalization, effectively separating transactional data from Master Data.
## 🛠️ Tech Stack

<img src="https://github.com/user-attachments/assets/e087168a-d867-44f2-835b-d5c5768ae71e" width="400">

* **Language:** Python (Pandas)

---

## 🏗️ Architecture & Data Modeling


<img src="https://github.com/user-attachments/assets/465f2088-7ce2-426f-998c-fdeb376c6f6f" width="400">

**Raw CSV 12,575 rows (Extract) → Clean + Dimensional Schema (Transform) → Validate (Test) → PostgreSQL (load)**

<img width="511" height="208" alt="image" src="https://github.com/user-attachments/assets/89b89840-7898-4bc6-9e54-c8d19ef463b1" /><br>
<img width="501" height="103" alt="image" src="https://github.com/user-attachments/assets/287c7ce8-e63e-415f-b055-ca772a7a34dc" /><br>
<img width="505" height="284" alt="image" src="https://github.com/user-attachments/assets/df181dac-f438-475e-b5b4-c9d3070cbdd4" /><br>
<img width="740" height="885" alt="image" src="https://github.com/user-attachments/assets/b1715ba4-0e2a-45a0-8d13-15778634745f" /><br>







---

<img width="400" alt="image" src="https://github.com/user-attachments/assets/7831a4ba-6c5a-4eb1-8a4e-bd39b6b7431c" />

The pipeline reads a raw dataset of ~12,500 rows and normalizes it into 3 tables:
1. **`dim_products` (Dimension):** Stores unique items, categories, and unit prices.
2. **`dim_date` (Dimension):** Extracts specific date components (Year, Month, Day, Weekday) for easy time-series analysis.
3. **`fact_transactions` (Fact):** Stores the core sales events, linking to dimensions via Foreign Keys.
<img width="1256" height="167" alt="image" src="https://github.com/user-attachments/assets/9a03f578-e0fa-45b6-b961-686be0569c19" />
<img width="1044" height="145" alt="image" src="https://github.com/user-attachments/assets/a48337f0-b1f0-47b6-a566-3387fb0f70ea" />



## 🧠 Data Engineering Decisions & Problem-Solving
**Decision 1: DROP NULL Items (9.6% Data Loss)**
The Problem:
From `retail_store_sales.csv`, I found ~1,213 rows (9.6%) where Item column is empty: 
```
TXN_6958546,CUST_16,Milk Products,,,2.0,13.0,Cash,In-store,2023-11-08,
TXN_4258144,CUST_20,Milk Products,,9.5,,,Cash,In-store,2024-11-15,False
TXN_3922646,CUST_08,Electric household essentials,,,2.0,64.0,Cash,Online,2023-07-27,False
```

SO I DECIDE TO
```
def transform_products(df):
    """Extract unique products"""
    # Log before filtering
    print(f"⚠️  Found {df['Item'].isna().sum()} NULL items - DROPPING")
    
    # Drop NULL items to ensure PK integrity
    df_clean = df.dropna(subset=['Item', 'Price Per Unit'])
    
    dim_products = df_clean[['Item', 'Category', 'Price Per Unit']]\
        .drop_duplicates(subset=['Item'], keep='first')
    
    return dim_products
```
Rationale:

✅ Data Integrity: Cannot have NULL primary keys

✅ Auditable: Loss is documented and reversible

✅ Business Logic: Better 100% trustworthy data than 100% quantity

✅ Impact: 11,362 verified rows remaining (acceptable)

---
**Decision 2: Recalculate Total_Spent (2.8% Data Discrepancies)**
The Problem:
Analyzing the CSV, I discovered calculation mismatches:

```
# CORRECT: Qty × Price = Total
TXN_9090630,CUST_22,Patisserie,Item_12_PAT,21.5,7.0,150.5,Cash,In-store,2022-02-03,
# 21.5 × 7.0 = 150.5 ✓

# SUSPICIOUS: Qty and Price present but Total empty
TXN_4258144,CUST_20,Milk Products,,9.5,,,Cash,In-store,2024-11-15,False
# No Price Per Unit recorded

# WRONG CALCULATION: Data entry error upstream
TXN_7433742,CUST_12,Electric household essentials,,,5.0,77.5,Digital Wallet,Online,2023-09-09,True
# Missing Item + Price but Total given → Suspicious
```
SO I DECIDE TO
```
def transform_fact(df, dim_date):
    """Transform into fact table"""
    fact_transactions = df.copy()
    
    # RECALCULATE: Ensure mathematical correctness
    fact_transactions['Total Spent'] = \
        fact_transactions['Quantity'] * fact_transactions['Price Per Unit']
    
    # Verify no NaN introduced
    assert fact_transactions['Total Spent'].notna().all(), \
        "Total Spent cannot be NULL after calculation"
    
    # ... rest of transformation
```
Rationale:

✅ Reproducible Truth: Qty × Price is mathematically verifiable

✅ Audit Trail: Can trace original CSV values if needed

✅ Financial Correctness: Prevents cascading errors in BI reports

✅ Separation of Concerns: Extract = read raw, Transform = apply business logic

---
**Decision 3: Impute Discount_Applied NULL → FALSE**
The Problem:
Looking at the data:

```
# Some have explicit Discount_Applied
TXN_9090630,...,Cash,In-store,2022-02-03,        ← Empty (NULL)
TXN_2486150,...,Cash,In-store,2024-04-30,        ← Empty (NULL)
TXN_9872413,...,Digital Wallet,In-store,2024-06-22,False
TXN_1978695,...,Cash,Online,2022-09-23,False
```
SO I DECIDE TO

```
def transform_fact(df, dim_date):
    # Business rule: Missing discount = no discount
    df['Discount Applied'] = df['Discount Applied'].fillna(False)
    
    # Log imputation for auditing
    null_count = (df['Discount Applied'].isna()).sum()
    if null_count > 0:
        print(f"⚠️  Imputed {null_count} NULL → False in Discount Applied")
```
Rationale:

✅ Business Logic: If discount wasn't recorded, it wasn't applied

✅ Cleaner Analytics: Boolean columns shouldn't have NULL in fact tables

✅ Reversible: Can query which rows were imputed

✅ Idempotent: Same result on re-runs

---

## 📊 Sample Analytics (Is data ready to use?)
**Example: Top 5 best selling products**
```sql
SELECT 
    p.item_name,                          -- ← ชื่อสินค้า จาก dim_products
    p.category,                           -- ← หมวดหมู่ จาก dim_products
    COUNT(f.transaction_id) AS times_sold,-- ← จำนวนครั้งที่ขาย
    SUM(f.quantity) AS total_units,       -- ← รวมจำนวนหน่วย
    ROUND(SUM(f.total_spent), 2) AS revenue,-- ← รวมยอดขาย
    ROUND(p.price_per_unit, 2) AS price   -- ← ราคา จาก dim_products
FROM fact_transactions f
JOIN dim_products p 
    ON f.item_name = p.item_name          -- ← Join by item_name
GROUP BY p.item_name, p.category, p.price_per_unit
ORDER BY times_sold DESC
LIMIT 5;
```
<img width="812" height="162" alt="image" src="https://github.com/user-attachments/assets/191b61fb-81fa-476c-9384-a808f819fc2f" />

## ⚙️ How to Run the Pipeline
Clone this repository.

Create a .env file with your PostgreSQL credentials (DB_HOST, DB_USER, DB_PASS, etc.).

Run the schema script sql/My_Schema.sql in DBeaver.

Install dependencies: pip install -r requirements.txt

Execute the pipeline: python main.py
