import os

from src.sqlite.setup_db import setup_database

if __name__ == "__main__":
    setup_database(database_path=os.getenv("DATABASE_FILEPATH"))
