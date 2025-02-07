import os

from src.fetch_data.get_data import scrape_data

if __name__ == "__main":
    if not os.path.exists("data/") and not os.path.isdir("data/"):
        os.makedirs(os.getenv("XML_PATH"))

    scrape_data(output_directory_path=os.getenv("XML_PATH"))
