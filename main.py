from code.crawl_data import crawl_ten_thousand_page

def main():
    FILE_NAME = "test1.jsonl"
    START_ID = 44891717
    crawl_ten_thousand_page(start_id = START_ID, file_name = FILE_NAME)

if __name__ == "__main__":
    main()