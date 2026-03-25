import json
import psycopg2
from psycopg2.extras import execute_values
import os

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
        print(f"Không tìm thấy file: {file_path}")
        return

    # Kết nối tới Database
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
    except Exception as e:
        print(f"❌ Lỗi kết nối Database: {e}")
        return

    batch_data = []
    success_count = 0
    
    # Câu lệnh SQL Upsert (Chèn hoặc Cập nhật nếu trùng ID)
    query = """
        INSERT INTO price_real_estate (listing_id, raw_data)
        VALUES %s
        ON CONFLICT (listing_id) 
        DO UPDATE SET raw_data = EXCLUDED.raw_data, crawled_at = CURRENT_TIMESTAMP;
    """

    print(f"⏳ Đang đọc và nạp file '{file_path}'...")
    
    # Mở file và đọc từng dòng
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            if not line.strip(): 
                continue
            try:
                data = json.loads(line)
                
                listing_id = data.get("listingId")
                
                if listing_id:
                    raw_data_json = json.dumps(data, ensure_ascii=False)
                    batch_data.append((listing_id, raw_data_json))
                    
                if len(batch_data) >= 1000:
                    execute_values(cursor, query, batch_data)
                    conn.commit() 
                    success_count += len(batch_data)
                    batch_data = [] 
                    print(f"Đã nạp thành công {success_count} dòng...")
                    
            except json.JSONDecodeError:
                continue # Bỏ qua nếu có dòng JSON bị lỗi định dạng
    
    # Đẩy nốt phần lẻ còn sót lại (ví dụ file có 10.300 dòng thì đẩy nốt 300 dòng cuối)
    if batch_data:
        execute_values(cursor, query, batch_data)
        conn.commit()
        success_count += len(batch_data)

    cursor.close()
    conn.close()
    print(f"Tổng cộng đã nạp {success_count} bất động sản vào PostgreSQL.")

if __name__ == "__main__":
    # Thay tên file này bằng tên file JSONL mà bạn vừa cào xong
    id = 45000000 - 9999 - 10000 * 5
    current_dir = os.path.dirname(os.path.abspath(__file__))
    while (id <= 44990001):
        FILE_CAN_NAP = os.path.join(current_dir, f"price_real_estate{id}.jsonl")
        load_generic_to_pg(FILE_CAN_NAP)
        id += 10000 