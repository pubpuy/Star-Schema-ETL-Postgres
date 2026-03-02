"""
Load Data into PostgreSQL Database
Loads transformed data with error handling and transaction control
"""

import os
import logging
from datetime import datetime
import psycopg2
from psycopg2 import sql, Error
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DatabaseLoader:
    def __init__(self):
        """Initialize database loader with config from .env"""
        self.host = os.getenv('DB_HOST')
        self.port = os.getenv('DB_PORT')
        self.database = os.getenv('DB_NAME')
        self.user = os.getenv('DB_USER')
        self.password = os.getenv('DB_PASS')
        self.connection = None
        self.cursor = None
        
    def connect(self):
        """Establish connection to PostgreSQL database"""
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            self.cursor = self.connection.cursor()
            logger.info(f"✓ Connected to PostgreSQL: {self.user}@{self.host}:{self.port}/{self.database}")
            return True
        except Error as e:
            logger.error(f"✗ Database connection failed: {e}")
            return False
    
    def disconnect(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")
    
    def truncate_tables(self):
        """Truncate all tables to prepare for fresh load"""
        try:
            logger.info("Truncating tables...")
            
            # Disable FK constraints temporarily
            self.cursor.execute("ALTER TABLE fact_transactions DISABLE TRIGGER ALL;")
            
            # Truncate tables
            tables = ['fact_transactions', 'dim_products', 'dim_date']
            for table in tables:
                self.cursor.execute(f'TRUNCATE TABLE "{table}" RESTART IDENTITY CASCADE;')
                logger.info(f"  ✓ Truncated {table}")
            
            # Re-enable FK constraints
            self.cursor.execute("ALTER TABLE fact_transactions ENABLE TRIGGER ALL;")
            
            self.connection.commit()
            logger.info("✓ All tables truncated successfully")
            print()
            return True
            
        except Error as e:
            logger.error(f"✗ Error truncating tables: {e}")
            self.connection.rollback()
            return False
    
    def load_dim_date(self, dim_date_df):
        """Load dimension date table"""
        try:
            logger.info(f"Loading dim_date ({len(dim_date_df)} rows)...")
            
            insert_query = sql.SQL("""
                INSERT INTO dim_date (date_id, full_date, year, month, day, weekday)
                VALUES (%s, %s, %s, %s, %s, %s)
            """)
            
            rows_inserted = 0
            for idx, row in dim_date_df.iterrows():
                self.cursor.execute(insert_query, (
                    int(row['date_id']),
                    row['full_date'],
                    int(row['year']),
                    int(row['month']),
                    int(row['day']),
                    row['weekday']
                ))
                rows_inserted += 1
            
            self.connection.commit()
            logger.info(f"  ✓ Inserted {rows_inserted} rows into dim_date")
            print()
            return True
            
        except Error as e:
            logger.error(f"✗ Error loading dim_date: {e}")
            self.connection.rollback()
            return False
    
    def load_dim_products(self, dim_products_df):
        """Load dimension products table"""
        try:
            logger.info(f"Loading dim_products ({len(dim_products_df)} rows)...")
            
            insert_query = sql.SQL("""
                INSERT INTO dim_products (item_name, category, price_per_unit)
                VALUES (%s, %s, %s)
            """)
            
            rows_inserted = 0
            for idx, row in dim_products_df.iterrows():
                self.cursor.execute(insert_query, (
                    row['item_name'],
                    row['category'],
                    float(row['price_per_unit'])
                ))
                rows_inserted += 1
            
            self.connection.commit()
            logger.info(f"  ✓ Inserted {rows_inserted} rows into dim_products")
            print()
            return True
            
        except Error as e:
            logger.error(f"✗ Error loading dim_products: {e}")
            self.connection.rollback()
            return False
    
    def load_fact_transactions(self, fact_transactions_df):
        """Load fact transactions table"""
        try:
            logger.info(f"Loading fact_transactions ({len(fact_transactions_df)} rows)...")
            
            insert_query = sql.SQL("""
                INSERT INTO fact_transactions 
                (date_id, customer_id, item_name, quantity, total_spent, discount_applied, payment_method, location)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """)
            
            rows_inserted = 0
            for idx, row in fact_transactions_df.iterrows():
                self.cursor.execute(insert_query, (
                    int(row['date_id']),
                    row['customer_id'],
                    row['item_name'],
                    int(row['quantity']),
                    float(row['total_spent']),
                    row['discount_applied'],
                    row['payment_method'],
                    row['location']
                ))
                rows_inserted += 1
            
            self.connection.commit()
            logger.info(f"  ✓ Inserted {rows_inserted} rows into fact_transactions")
            print()
            return True
            
        except Error as e:
            logger.error(f"✗ Error loading fact_transactions: {e}")
            self.connection.rollback()
            return False
    
    def verify_load(self):
        """Verify data was loaded correctly"""
        try:
            logger.info("Verifying data load...")
            
            queries = {
                'dim_date': 'SELECT COUNT(*) FROM dim_date;',
                'dim_products': 'SELECT COUNT(*) FROM dim_products;',
                'fact_transactions': 'SELECT COUNT(*) FROM fact_transactions;'
            }
            
            for table, query in queries.items():
                self.cursor.execute(query)
                count = self.cursor.fetchone()[0]
                logger.info(f"  ✓ {table}: {count} rows")
            
            print()
            return True
            
        except Error as e:
            logger.error(f"✗ Error verifying data: {e}")
            return False


def load_data(dim_date, dim_products, fact_transactions):
    """
    Main load function
    Load transformed data into PostgreSQL database
    """
    
    logger.info("=" * 70)
    logger.info("เริ่มต้นกระบวนการ Load ข้อมูลเข้า PostgreSQL Database")
    logger.info("=" * 70)
    
    # Initialize loader
    loader = DatabaseLoader()
    
    # Step 1: Connect to database
    logger.info("📡 ขั้นตอนที่ 1: เชื่อมต่อ Database")
    logger.info("-" * 70)
    if not loader.connect():
        logger.error("❌ ไม่สามารถเชื่อมต่อ Database - หยุดการทำงาน")
        return False
    
    print()
    
    # Step 2: Truncate tables
    logger.info("🗑️  ขั้นตอนที่ 2: ลบข้อมูลเก่า (Truncate)")
    logger.info("-" * 70)
    if not loader.truncate_tables():
        loader.disconnect()
        return False
    
    # Step 3: Load dimension tables
    logger.info("📥 ขั้นตอนที่ 3: Load Dimension Tables")
    logger.info("-" * 70)
    
    if not loader.load_dim_date(dim_date):
        loader.disconnect()
        return False
    
    if not loader.load_dim_products(dim_products):
        loader.disconnect()
        return False
    
    print()
    
    # Step 4: Load fact table
    logger.info("📥 ขั้นตอนที่ 4: Load Fact Table")
    logger.info("-" * 70)
    
    if not loader.load_fact_transactions(fact_transactions):
        loader.disconnect()
        return False
    
    print()
    
    # Step 5: Verify data
    logger.info("✅ ขั้นตอนที่ 5: ตรวจสอบข้อมูล")
    logger.info("-" * 70)
    if not loader.verify_load():
        loader.disconnect()
        return False
    
    print()
    
    # Close connection
    loader.disconnect()
    
    logger.info("=" * 70)
    logger.info("✅ Load ข้อมูลเข้า Database เสร็จสำเร็จ!")
    logger.info("=" * 70)
    
    return True


if __name__ == "__main__":
    # Note: This would typically be called from main.py after extraction and transformation
    logger.info("Run this function from main.py using load_data(dim_date, dim_products, fact_transactions)")
