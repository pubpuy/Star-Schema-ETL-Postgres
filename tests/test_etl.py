# สมมติว่ารับค่า df ทั้ง 3 มาจาก transform แล้ว
# dim_date, dim_products, fact_transactions

def test_data_quality(dim_date, dim_products, fact_transactions):
    print("---- เริ่มการตรวจสอบข้อมูล (Data Validation) ----\n")

    try:
        # ============== 1. ตรวจสอบ dim_date ==============
        print("📋 ตรวจสอบ dim_date...")
        
        # ตรวจชื่อ columns ตามที่ SQL Schema บอก
        expected_date_cols = ['date_id', 'full_date', 'year', 'month', 'day', 'weekday']
        assert list(dim_date.columns) == expected_date_cols, \
            f"Error: ชื่อคอลัมน์ dim_date ไม่ตรง! คาดหวัง: {expected_date_cols}, พบ: {list(dim_date.columns)}"
        
        # date_id: PRIMARY KEY (ต้อง unique + NOT NULL)
        assert dim_date['date_id'].is_unique, "Error: date_id ใน dim_date มีค่าซ้ำ!"
        assert dim_date['date_id'].notnull().all(), "Error: พบค่า Null ใน date_id!"
        
        # full_date: UNIQUE + NOT NULL (ตามที่ SQL Schema บอก)
        assert dim_date['full_date'].is_unique, "Error: full_date ใน dim_date มีค่าซ้ำ!"
        assert dim_date['full_date'].notnull().all(), "Error: พบค่า Null ใน full_date!"
        
        # year, month, day, weekday ต้อง NOT NULL
        assert dim_date['year'].notnull().all(), "Error: พบค่า Null ใน year!"
        assert dim_date['month'].notnull().all(), "Error: พบค่า Null ใน month!"
        assert dim_date['day'].notnull().all(), "Error: พบค่า Null ใน day!"
        assert dim_date['weekday'].notnull().all(), "Error: พบค่า Null ใน weekday!"
        
        # month ต้องอยู่ระหว่าง 1-12
        assert ((dim_date['month'] >= 1) & (dim_date['month'] <= 12)).all(), "Error: month ต้องอยู่ระหว่าง 1-12!"
        
        # day ต้องอยู่ระหว่าง 1-31
        assert ((dim_date['day'] >= 1) & (dim_date['day'] <= 31)).all(), "Error: day ต้องอยู่ระหว่าง 1-31!"
        
        print("✓ dim_date: ผ่านการตรวจสอบ\n")

        # ============== 2. ตรวจสอบ dim_products ==============
        print("📋 ตรวจสอบ dim_products...")
        
        # ตรวจชื่อ columns ตามที่ SQL Schema บอก
        expected_product_cols = ['item_name', 'category', 'price_per_unit']
        assert list(dim_products.columns) == expected_product_cols, \
            f"Error: ชื่อคอลัมน์ dim_products ไม่ตรง! คาดหวัง: {expected_product_cols}, พบ: {list(dim_products.columns)}"
        
        # item_name: PRIMARY KEY (ต้อง unique + NOT NULL)
        assert dim_products['item_name'].is_unique, "Error: item_name ใน dim_products มีค่าซ้ำ (ต้องเป็น PK)!"
        assert dim_products['item_name'].notnull().all(), "Error: พบค่า Null ใน item_name!"
        
        # category: NOT NULL
        assert dim_products['category'].notnull().all(), "Error: พบค่า Null ใน category!"
        
        # price_per_unit: DECIMAL(10,2) ต้อง > 0 (ราคาไม่สามารถ ≤ 0 ได้)
        assert (dim_products['price_per_unit'] > 0).all(), "Error: price_per_unit ต้องมากกว่า 0!"
        assert dim_products['price_per_unit'].notnull().all(), "Error: พบค่า Null ใน price_per_unit!"
        
        print("✓ dim_products: ผ่านการตรวจสอบ\n")

        # ============== 3. ตรวจสอบ fact_transactions ==============
        print("📋 ตรวจสอบ fact_transactions...")
        
        # ตรวจชื่อ columns ตามที่ SQL Schema บอก
        expected_fact_cols = ['date_id', 'customer_id', 'item_name', 'quantity', 
                              'total_spent', 'discount_applied', 'payment_method', 'location']
        assert list(fact_transactions.columns) == expected_fact_cols, \
            f"Error: ชื่อคอลัมน์ fact_transactions ไม่ตรง! คาดหวัง: {expected_fact_cols}, พบ: {list(fact_transactions.columns)}"
        
        # date_id: FOREIGN KEY (ต้องอ้างอิงถึง dim_date.date_id)
        valid_date_ids = set(dim_date['date_id'].unique())
        assert fact_transactions['date_id'].isin(valid_date_ids).all(), \
            "Error: พบ date_id ใน fact_transactions ที่ไม่มีอยู่ใน dim_date (Foreign Key Violation)!"
        assert fact_transactions['date_id'].notnull().all(), "Error: พบค่า Null ใน date_id!"
        
        # customer_id: NOT NULL
        assert fact_transactions['customer_id'].notnull().all(), "Error: พบค่า Null ใน customer_id!"
        
        # item_name: FOREIGN KEY (ต้องอ้างอิงถึง dim_products.item_name)
        valid_items = set(dim_products['item_name'].unique())
        assert fact_transactions['item_name'].isin(valid_items).all(), \
            "Error: พบ item_name ใน fact_transactions ที่ไม่มีอยู่ใน dim_products (Foreign Key Violation)!"
        assert fact_transactions['item_name'].notnull().all(), "Error: พบค่า Null ใน item_name!"
        
        # quantity: INTEGER, ต้อง > 0
        assert (fact_transactions['quantity'] > 0).all(), "Error: quantity ต้องมากกว่า 0!"
        assert fact_transactions['quantity'].notnull().all(), "Error: พบค่า Null ใน quantity!"
        
        # total_spent: DECIMAL(12,2), ต้อง ≥ 0
        assert (fact_transactions['total_spent'] >= 0).all(), "Error: total_spent ไม่สามารถติดลบได้!"
        assert fact_transactions['total_spent'].notnull().all(), "Error: พบค่า Null ใน total_spent!"
        
        # discount_applied: BOOLEAN (ค่าอื่นๆ ปล่อยไป)
        # payment_method: VARCHAR(50) (ปล่อยไป - อาจมี Null ได้)
        # location: VARCHAR(50) (ปล่อยไป - อาจมี Null ได้)
        
        print("✓ fact_transactions: ผ่านการตรวจสอบ\n")
        
        print("=" * 60)
        print("✅ ข้อมูลทั้งหมดถูกต้องตรงกับ SQL Schema!")
        print("=" * 60)
        return True
        
    except AssertionError as e:
        print(f"\n❌ Validation Failed: {e}")
        raise

# วิธีเรียกใช้ (หลังจาก transform เสร็จ)
# test_data_quality(dim_date, dim_products, fact_transactions)