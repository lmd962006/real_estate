import psycopg2
import numpy as np
from sklearn.neighbors import BallTree
import pandas as pd

DB_CONFIG = {
    "dbname": "realestate_db",
    "user": "postgres",
    "password": "123456",
    "host": "localhost",
    "port": "5432"
}

ball_tree_house = None
coordinates = []
building = []
coordinates_temp = []

def build_up_tree():
    global ball_tree_house
    coor_radian = np.radians(coordinates)
    ball_tree_house = BallTree(coor_radian, metric='haversine')

def search_in_rad(design_coord: list[tuple[float, float]], design_distance: float):
    design_coord_rad = np.radians(design_coord)
    design_distance_rad = design_distance / 6371.0
    indices = ball_tree_house.query_radius(design_coord_rad, r=design_distance_rad)
    return indices

def give_data_batdongsan():
    global coordinates, building
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        query = """
            SELECT 
                listing_id,
                raw_data ->> 'title' AS title,
                CAST(raw_data ->> 'longitude' AS FLOAT) AS longitude,
                CAST(raw_data ->> 'latitude' AS FLOAT) AS latitude,
                CAST(raw_data ->> 'computedPrice' AS FLOAT) AS computedPrice,
                CAST(raw_data ->> 'area' AS FLOAT) AS area
            FROM generic_real_estate
            WHERE raw_data -> 'latitude' IS NOT NULL
            LIMIT 30000
        """
        cursor.execute(query)
        records = cursor.fetchall()
        print(f"\nĐã lấy thành công {len(records)} căn nhà!\n")
        for row in records:
            # Lôi từng món đồ trong 1 dòng ra
            building.append((row[0], row[1], row[2], row[3], row[4], row[5], 'batdongsan'))
            longitude = row[2]
            latitude = row[3]
            coordinates_temp.append((latitude, longitude))
    except Exception as e:
        print(f"Có lỗi: {e}")

def give_data_meey():
    global coordinates, building
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        query = """
            SELECT 
                listing_id,
                raw_data ->> 'metaKeywords' AS metaKeywords,
                CAST(raw_data -> 'location' -> 'coordinates' ->> 0 AS FLOAT) AS longitude,
                CAST(raw_data -> 'location' -> 'coordinates' ->> 1 AS FLOAT) AS latitude,
                CAST(raw_data ->> 'totalPrice' AS FLOAT) totalPrice,
                CAST(raw_data ->> 'area' AS FLOAT)  AS area
            FROM meey_estate
            WHERE raw_data -> 'location' -> 'coordinates' IS NOT NULL
            LIMIT 30000
        """
        cursor.execute(query)
        records = cursor.fetchall()
        print(f"\nĐã lấy thành công {len(records)} căn nhà!\n")
        for row in records:
            # Lôi từng món đồ trong 1 dòng ra
            building.append((row[0], row[1], row[2], row[3], row[4], row[5], 'meey'))
            longitude = row[2]
            latitude = row[3]
            coordinates_temp.append((latitude, longitude))
    except Exception as e:
        print(f"Có lỗi: {e}")

def give_data_hospital():
    latitude: float
    longitude: float
    all_hospital = pd.read_csv('benh_vien_viet_nam.csv')
    for index, row in all_hospital.iterrows():
        name = row['Hospital_name']
        latitude = row['Latitude']
        longitude = row['Longitude']
        building.append((index, name, latitude, longitude, 0, 0, 'hospital'))
        coordinates_temp.append((latitude, longitude))

def count_hospital(result: np.ndarray, typebuild: str):
    res = 0
    build_in_range = []
    for id in result:
        if (building[id][-1] == typebuild):
            build_in_range.append(building[id][1])
            res += 1
    print(f"Trong khu vực có tổng cộng {res} tòa theo yêu cầu")
    print(build_in_range)

def estimate_price(latitude: float, longitude: float, area):
    test = [[latitude, longitude]]
    building_in_range_tmp = search_in_rad(test, 0.5)
    building_in_range = building_in_range_tmp[0]
    price_per_metre = []
    for id in building_in_range:
        if building[id][5] != 0:
            price_per_metre.append(building[id][4] / building[id][5])
    #Chia theo tu phan vi
    building_number = len(price_per_metre)
    sorted(price_per_metre)
    left_price = building_number // 4
    right_price = max((3 * building_number) // 4, left_price + 1)
    print(price_per_metre[left_price] * area, price_per_metre[right_price] * area)

if __name__ == "__main__":
    give_data_batdongsan()
    give_data_meey()
    give_data_hospital()
    coordinates = np.array(coordinates_temp)
    build_up_tree()
    estimate_price(11.0303912, 106.70090001, 100)
    #res = search_in_rad(test, 1)
    #count_hospital(res[0], 'meey')
