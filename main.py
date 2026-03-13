from code.crawl_data import crawl_ten_thousand_page_of_generic
from code.crawl_data import crawl_ten_thousand_page_of_price

def main():
    START_ID = 39000001
    while START_ID <= 45000000 - 9999:
        FILE_NAME_GENERIC = f"crawldata\generic_real_estate{START_ID}.jsonl"
        FILE_NAME_PRICE = f"crawldata\price_real_estate{START_ID}.jsonl"
        crawl_ten_thousand_page_of_generic(start_id=START_ID, file_name=FILE_NAME_GENERIC)
        crawl_ten_thousand_page_of_price(start_id=START_ID, file_name=FILE_NAME_PRICE)
        START_ID += 100000


if __name__ == "__main__":
    main()