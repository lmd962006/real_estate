import requests
import json
from DrissionPage import SessionPage
import time
import concurrent.futures
from curl_cffi import requests as cureq
lmao = 1

fake_page = SessionPage()

def crawl_a_page(api_url: str):
    time.sleep(1)
    try:
        fake_page.get(api_url)
        
        if fake_page.response.status_code == 200:
            json_response = fake_page.json 
            
            if isinstance(json_response, dict) and json_response.get("isSuccess"):
                json_data = json_response.get("data")
                if json_data:
                    # Bơm ID vào
                    if "pricing-histories" in api_url:
                        json_data["listingId"] = int(api_url.split('/listings/')[1].split('/')[0])
                    else:
                        json_data["listingId"] = int(api_url.split('/listings/')[1])
                    return json_data
            return None
            
        elif fake_page.response.status_code == 429:
            return None
        else:
            return None
            
    except Exception as e:
        print(f"Lỗi trong quá trình chạy: {e}")
        return None

def crawl_ten_thousand_page_of_generic(start_id: int, file_name: str):
    urls = [
        f"https://api.batdongsan.com.vn/bff-consumer-mobile/api/v2/listings/{listing_id}" 
        for listing_id in range(start_id, start_id + 10000)
    ]
    with open(file_name, 'a', encoding='utf-8') as f:
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(crawl_a_page, url): url for url in urls}
            for future in concurrent.futures.as_completed(futures):
                data = future.result()
                if data:
                    json_string = json.dumps(data, ensure_ascii=False)
                    f.write(json_string + '\n')

def crawl_ten_thousand_page_of_price(start_id: int, file_name: str):
    urls = [
        f"https://api.batdongsan.com.vn/bff-consumer-mobile/api/v1/listings/{listing_id}/pricing-histories?countOfYears=5&listingType=38" 
        for listing_id in range(start_id, start_id + 10000)
    ]
    with open(file_name, 'a', encoding='utf-8') as f:
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            futures = {executor.submit(crawl_a_page, url): url for url in urls}
            for future in concurrent.futures.as_completed(futures):
                data = future.result()
                if data:
                    json_string = json.dumps(data, ensure_ascii=False)
                    f.write(json_string + '\n')
