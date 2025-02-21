import os

from src.fetch_data.get_data import scrape_data

if __name__ == "__main__":
    if os.path.exists(os.getenv("XML_PATH")) and not os.path.isdir(
        os.getenv("XML_PATH")
    ):
        raise ValueError(f"path {os.getenv("XML_PATH")} already exists")
    os.makedirs(os.getenv("XML_PATH"), exist_ok=True)

    scrape_data(output_directory_path=os.getenv("XML_PATH"))
