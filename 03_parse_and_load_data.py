import os
from src.preprocessing.parse_data import PlenarprotokollXMLParser
from src.sqlite.load_data_into_db import load_data_into_db


if __name__ == "__main__":
    parser = PlenarprotokollXMLParser()

    parser.crawl_directory(
        input_directory_path=os.getenv("XML_PATH"),
        output_directory_path=os.getenv("JSON_PATH"),
    )

    load_data_into_db(
        json_directory_path=os.getenv("JSON_PATH"),
        database_path=os.getenv("DATABASE_FILEPATH"),
    )
