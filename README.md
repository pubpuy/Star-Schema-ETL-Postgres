## 📌 Project Overview (โปรเจกต์นี้ทำอะไร?)
This project is an end-to-end **ETL (Extract, Transform, Load) Pipeline** designed to process raw, "dirty" retail sales data and load it into a structured Relational Database.

## 💡 Business Value (โปรเจกต์นี้ช่วยอะไรได้บ้าง?)
* **Single Source of Truth:** เปลี่ยนจากไฟล์ CSV ที่กระจัดกระจายและมีข้อผิดพลาด ให้กลายเป็น Database ที่เชื่อถือได้
* **Data Quality & Integrity:** ป้องกันข้อมูลขยะเข้าสู่ระบบ โดยมีการกรองข้อมูล Null และคำนวณยอดขายใหม่ (Recalculate) ให้ถูกต้อง 100% ก่อนนำไปใช้งาน
* **Optimized for Analytics:** โครงสร้าง Star Schema ช่วยให้ Data Analyst หรือ BI Tools สามารถดึงข้อมูล (Query) สรุปยอดขายตามมิติเวลา (Time) หรือหมวดหมู่สินค้า (Category) ได้รวดเร็วและเขียน SQL ได้ง่ายขึ้นมาก
* **Storage Efficiency:** ลดความซ้ำซ้อนของข้อมูล (Data Redundancy) ด้วยการทำ Normalization แยกข้อมูล Master Data ออกมา

## 🛠️ Tech Stack

