import json
import psycopg2
from psycopg2.extras import execute_values
import os
import time

#Cấu hình thông tin database
DB_CONFIG = {
    "dbname": "realestate_db",   # Đổi thành tên Database của bạn
    "user": "postgres",          # Username của bạn (thường là postgres)
    "password": "123456", # Mật khẩu của bạn
    "host": "localhost",         # Giữ nguyên nếu chạy trên cùng máy
    "port": "5432"               # Giữ nguyên nếu dùng port mặc định
}

def load_generic_to_pg(file_path: str):
    if not os.path.exists(file_path):
        print("File không tồn tại")
        return
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
    except Exception as e:
        print(e)
    batch_data = []
    count_charge = 0
    query = """
        INSERT INTO meey_estate (listing_id, raw_data)
        VALUES %s
        ON CONFLICT (listing_id) 
        DO UPDATE SET raw_data = EXCLUDED.raw_data, crawled_at = CURRENT_TIMESTAMP;
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            if not line.strip():
                continue
            try:
                # Đọc cả dòng JSON
                data = json.loads(line)
                
                # Trích xuất riêng cái "nhân" chứa data bất động sản
                
                if isinstance(data, dict): # Đảm bảo "nhân" có tồn tại
                    # Móc đúng cái mã code 305... ra làm ID
                    listing_id = data.get("code") 
                    
                    if listing_id:
                        # Đóng gói cái "nhân" lại để nhét vào cột raw_data (bỏ qua cái vỏ 200)
                        raw_data_json = json.dumps(data, ensure_ascii=False)
                        batch_data.append((listing_id, raw_data_json))
                    if len(batch_data) >= 1000:
                        execute_values(cursor, query, batch_data)
                        conn.commit() 
                        count_charge += len(batch_data)
                        batch_data = [] 
                        print(f"Đã nạp thành công {count_charge} dòng...")
            except json.JSONDecodeError:
                continue
    if batch_data:
        execute_values(cursor, query, batch_data)
        conn.commit()
        count_charge += len(batch_data)

    cursor.close()
    conn.close()
    print(f"Tổng cộng đã nạp {count_charge} bất động sản vào PostgreSQL.")

if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))
    id = 305930001
    while (id <= 305980001):
        FILE_CAN_NAP = os.path.join(current_dir, f"meeyproject{id}.jsonl")
        load_generic_to_pg(FILE_CAN_NAP)
        id += 10000 