import requests
import csv

def crawl_vietnam_hospitals():
    print("Đang gửi yêu cầu đến Overpass API... (Có thể mất vài chục giây)")
    overpass_url = "http://overpass-api.de/api/interpreter"
    
    # Câu lệnh Overpass QL: Lấy khu vực VN và tìm tất cả node/way/relation là bệnh viện
    overpass_query = """
    [out:json][timeout:90];
    area["ISO3166-1"="VN"]->.searchArea;
    (
      node["amenity"="hospital"](area.searchArea);
      way["amenity"="hospital"](area.searchArea);
      relation["amenity"="hospital"](area.searchArea);
    );
    out center;
    """
    
    response = requests.post(overpass_url, data={'data': overpass_query})
    
    if response.status_code != 200:
        print("Lỗi khi tải dữ liệu:", response.status_code)
        return
        
    data = response.json()
    hospitals = []
    
    # Lọc dữ liệu lấy Tên, Vĩ độ (Lat) và Kinh độ (Lon)
    for element in data.get('elements', []):
        tags = element.get('tags', {})
        name = tags.get('name', 'Chưa có tên')
        
        # Với node thì có luôn lat/lon, với way/relation thì lấy ở trường center
        if element['type'] == 'node':
            lat, lon = element['lat'], element['lon']
        elif 'center' in element:
            lat, lon = element['center']['lat'], element['center']['lon']
        else:
            continue
            
        hospitals.append([name, lat, lon])
        
    print(f"Đã tìm thấy {len(hospitals)} bệnh viện!")
    
    # Lưu ra file CSV để dễ mở bằng Excel
    filename = "benh_vien_viet_nam.csv"
    with open(filename, mode='w', encoding='utf-8-sig', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Hospital_name', 'Latitude', 'Longitude'])
        writer.writerows(hospitals)
        
    print(f"Đã lưu toàn bộ dữ liệu vào file: {filename}")

if __name__ == "__main__":
    crawl_vietnam_hospitals()