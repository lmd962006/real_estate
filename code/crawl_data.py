import requests
import json
import concurrent.futures

def crawl_a_page(api_url: str):
    # Đóng giả làm một trình duyệt thật trên điện thoại hoặc máy tính
    fake_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json"
    }
    
    try:
        # THÊM TIMEOUT: Chờ kết nối 3s, chờ đọc dữ liệu tối đa 5s. Quá thời gian -> Văng lỗi rồi chạy tiếp.
        response = requests.get(api_url, headers=fake_headers, timeout=(3, 5))
        
        # Nếu server trả về lỗi 403 (Bị cấm) hoặc 404 (Không tìm thấy), nó sẽ báo ngay
        if response.status_code != 200:
            print(f"Bỏ qua URL (Mã lỗi {response.status_code}): {api_url}")
            return
            
        json_response = response.json()
        
        if json_response.get("isSuccess") == True:
            json_data = json_response.get("data")
            return json_data

    except requests.exceptions.Timeout:
        print(f"Quá thời gian chờ (Timeout) ở link: {api_url}")
    except Exception as e:
        print(f"Lỗi khác ở link {api_url}: {e}")

def crawl_ten_thousand_page(start_id: int, file_name: str):
    urls = [
        f"https://api.batdongsan.com.vn/bff-consumer-mobile/api/v2/listings/{listing_id}" 
        for listing_id in range(start_id, start_id + 10000)
    ]
    with open(file_name, 'a', encoding='utf-8') as f:
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(crawl_a_page, url): url for url in urls}
            for future in concurrent.futures.as_completed(futures):
                data = future.result()
                saved = 0
                if data:
                    json_string = json.dumps(data, ensure_ascii=False)
                    f.write(json_string + '\n')
                    saved += 1
                    if saved % 100 == 0:
                        print(f"Đã lưu thành công {saved} file")