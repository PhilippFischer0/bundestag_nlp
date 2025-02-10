import sqlite3

# SQL statements to initialize the database tables
sql_statements = [
    """CREATE TABLE IF NOT EXISTS sitzungen(
            sitzungs_id INTEGER PRIMARY KEY NOT NULL,
            datum DATE NOT NULL,
            start TIME NOT NULL,
            ende TIME NOT NULL
        );""",
    """CREATE TABLE IF NOT EXISTS tagesordnungspunkte(
            tagesordnungspunkt_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            sitzungs_id INTEGER NOT NULL,
            FOREIGN KEY (sitzungs_id) REFERENCES sitzungen (sitzungs_id)
        );""",
    """CREATE TABLE IF NOT EXISTS redner(
            redner_id INTEGER PRIMARY KEY NOT NULL,
            titel TEXT,
            vorname TEXT NOT NULL,
            nachname TEXT NOT NULL,
            fraktion TEXT
        );""",
    """CREATE TABLE IF NOT EXISTS rollen(
            rollen_id INTEGER PRIMARY KEY,
            beschreibung TEXT NOT NULL
        );""",
    """CREATE TABLE IF NOT EXISTS reden(
            rede_id TEXT PRIMARY KEY NOT NULL,
            text TEXT NOT NULL,
            redner_id INTEGER NOT NULL,
            tagesordnungspunkt_id INTEGER NOT NULL,
            rollen_id INTEGER,
            FOREIGN KEY (redner_id) REFERENCES redner (redner_id),
            FOREIGN KEY (tagesordnungspunkt_id) REFERENCES tagesordnungspunkte (tagesordnungspunkt_id),
            FOREIGN KEY (rollen_id) REFERENCES rollen (rollen_id)
        );""",
    """CREATE TABLE IF NOT EXISTS kommentare(
            kommentar_id INTEGER PRIMARY KEY,
            kommentar_index INTEGER NOT NULL,
            kommentator TEXT NOT NULL,
            fraktion TEXT NOT NULL,
            text TEXT NOT NULL,
            rede_id TEXT NOT NULL,
            FOREIGN KEY (rede_id) REFERENCES reden (rede_id),
            UNIQUE (kommentar_index, rede_id)
        );""",
]


def setup_database(database_path: str) -> None:
    try:
        # connect to and create database
        with sqlite3.connect(database_path) as conn:
            print("database created")

        try:
            # create tables in the database by iterating through the SQL statements defined above
            print("creating tables...")
            cursor = conn.cursor()

            for statement in sql_statements:
                cursor.execute(statement)

            conn.commit()
            print("tables created successfully")
        except sqlite3.OperationalError as e:
            print("Failed to create tables:", e)

    except sqlite3.OperationalError as e:
        print("Failed to open database:", e)
