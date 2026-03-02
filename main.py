"""
ETL Pipeline - Main Entry Point
Extracts data, transforms it, runs validation tests, and loads into PostgreSQL
"""

import sys
import os

# ตั้งค่า path ให้ชี้ไปที่ project root
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pandas as pd
from src.extract import extract_data
from src.transform import transform_products, transform_date, transform_fact
from src.load import load_data
from tests.test_etl import test_data_quality

def main():
    """
    Main ETL Pipeline:
    1. Extract data from CSV
    2. Transform data into dimensional tables
    3. Run data quality tests
    4. Load data into PostgreSQL database
    """
    
    print("=" * 70)
    print("เริ่มต้นกระบวนการ ETL Pipeline (Extract → Transform → Test → Load)")
    print("=" * 70 + "\n")
    
    # Step 1: Extract
    print("📥 ขั้นตอนที่ 1: แยกข้อมูล (Extract)")
    print("-" * 70)
    DATA_FILE = "data/retail_store_sales.csv"
    raw_df = extract_data(DATA_FILE)
    
    if raw_df is None:
        print("❌ ไม่สามารถดึงข้อมูล - หยุดการทำงาน")
        return False
    
    print()
    
    # Step 2: Transform
    print("🔄 ขั้นตอนที่ 2: แปลงข้อมูล (Transform)")
    print("-" * 70)
    
    dim_date = transform_date(raw_df)
    print(f"✓ สร้าง dim_date: {len(dim_date)} แถว")
    
    dim_products = transform_products(raw_df)
    print(f"✓ สร้าง dim_products: {len(dim_products)} แถว")
    
    fact_transactions = transform_fact(raw_df, dim_date)
    print(f"✓ สร้าง fact_transactions: {len(fact_transactions)} แถว")
    
    print()
    
    # Step 3: Test Data Quality
    print("🧪 ขั้นตอนที่ 3: ตรวจสอบคุณภาพข้อมูล (Test)")
    print("-" * 70)
    
    try:
        test_data_quality(dim_date, dim_products, fact_transactions)
        
    except AssertionError as e:
        print()
        print("=" * 70)
        print(f"❌ Test ล้มเหลว: {e}")
        print("❌ ข้อมูลไม่ผ่านการตรวจสอบ - ยกเลิกการ Load")
        print("=" * 70)
        return False
    
    print()
    
    # Step 4: Load into Database
    print("💾 ขั้นตอนที่ 4: Load เข้า PostgreSQL Database")
    print("-" * 70)
    
    if not load_data(dim_date, dim_products, fact_transactions):
        print()
        print("=" * 70)
        print("❌ ไม่สามารถ Load ข้อมูลเข้า Database")
        print("=" * 70)
        return False
    
    print()
    print("=" * 70)
    print("✅ ETL Pipeline เสร็จสำเร็จ!")
    print("   ข้อมูลทั้งหมดได้ Load เข้า PostgreSQL Database แล้ว")
    print("=" * 70)
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
