import psycopg2
import numpy as np
from sklearn.neighbors import BallTree

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
                raw_data ->> 'districtName' AS district,
                CAST(raw_data ->> 'longitude' AS FLOAT) AS longitude,
                CAST(raw_data ->> 'latitude' AS FLOAT) AS latitude
            FROM generic_real_estate
            WHERE raw_data -> 'latitude' IS NOT NULL
            LIMIT 30000
        """
        cursor.execute(query)
        records = cursor.fetchall()
        print(f"\n✅ Đã lấy thành công {len(records)} căn nhà!\n")
        for row in records:
            # Lôi từng món đồ trong 1 dòng ra
            building.append((row[0], row[1], row[2], row[3], 'batdongsan'))
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
                raw_data -> 'district' ->> 'slug' AS district,
                CAST(raw_data -> 'location' -> 'coordinates' ->> 0 AS FLOAT) AS longitude,
                CAST(raw_data -> 'location' -> 'coordinates' ->> 1 AS FLOAT) AS latitude
            FROM meey_estate
            WHERE raw_data -> 'location' -> 'coordinates' IS NOT NULL
            LIMIT 30000
        """
        cursor.execute(query)
        records = cursor.fetchall()
        print(f"\n✅ Đã lấy thành công {len(records)} căn nhà!\n")
        for row in records:
            # Lôi từng món đồ trong 1 dòng ra
            building.append((row[0], row[1], row[2], row[3], 'meey'))
            longitude = row[2]
            latitude = row[3]
            coordinates_temp.append((latitude, longitude))
    except Exception as e:
        print(f"Có lỗi: {e}")

if __name__ == "__main__":
    give_data_batdongsan()
    give_data_meey()
    coordinates = np.array(coordinates_temp)
    build_up_tree()
    test = [coordinates[0]]
    res = search_in_rad(test, 20)
    print(res[0])