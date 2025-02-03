import os
from src.preprocessing.parse_data import PlenarprotokollXMLParser
from src.sqlite.load_data_into_db import load_data_into_db


parser = PlenarprotokollXMLParser()

parser.crawl_directory()

load_data_into_db()
