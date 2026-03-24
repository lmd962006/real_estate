from code.crawl_data import crawl_ten_thousand_page_of_generic
from code.crawl_data import crawl_ten_thousand_page_of_price
from code.crawl_data import crawl_ten_thousand_page_of_meey

def crawl_meey(START_ID: int, step: int):
    id = START_ID
    while (step > 0):
        step -= 1
        FILE_NAME_PRICE = f"crawlmeey\meeyproject{id}.jsonl"
        crawl_ten_thousand_page_of_meey(start_id=id, file_name=FILE_NAME_PRICE)
        id += 10000

def crawl_batdongsan(START_ID: int, step: int):
    while step > 0:
        step -= 1
        FILE_NAME_GENERIC = f"crawldata\generic_real_estate{START_ID}.jsonl"
        FILE_NAME_PRICE = f"crawldata\price_real_estate{START_ID}.jsonl"
        crawl_ten_thousand_page_of_generic(start_id=START_ID, file_name=FILE_NAME_GENERIC)
        crawl_ten_thousand_page_of_price(start_id=START_ID, file_name=FILE_NAME_PRICE)
        START_ID += 100000

def main():
    #crawl_batdongsan(39000001)
    crawl_meey(305960001, 3)


if __name__ == "__main__":
    main()