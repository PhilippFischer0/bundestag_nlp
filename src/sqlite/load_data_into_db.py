import json
import os
import sqlite3


def load_data_into_db(json_directory_path: str, database_path: str) -> None:
    # load file data
    with open(
        os.path.join(json_directory_path, "data.json"), "rt", encoding="utf-8"
    ) as file:
        data = json.load(file)
    with open(
        os.path.join(json_directory_path, "redner.json"), "rt", encoding="utf-8"
    ) as file:
        redner = json.load(file)
    with open(
        os.path.join(json_directory_path, "rollen.json"), "rt", encoding="utf-8"
    ) as file:
        rollen = json.load(file)
        # connect to the database and initialize cursor to execute SQL statements
        with sqlite3.connect(database_path) as conn:
            cursor = conn.cursor()

            # iterate through the roles and add them to the relevant table
            for key, rolle in rollen["rollen"].items():
                cursor.execute(
                    """
                    INSERT INTO rollen (rollen_id, beschreibung)
                    VALUES (?, ?)
                """,
                    (int(key), rolle),
                )

            # iterate through unique_redner_list and add each item to the redner table
            for redner_key, redner_value in redner.items():
                cursor.execute(
                    """
                    INSERT INTO redner (redner_id, titel, vorname, nachname, fraktion)
                    VALUES (?, ?, ?, ?, ?)
                """,
                    (
                        int(redner_key),
                        redner_value.get("titel"),
                        redner_value["vorname"],
                        redner_value["nachname"],
                        redner_value.get("fraktion"),
                    ),
                )

            # iterate through the sitzungen and add the relevant metadata to the database
            for sitzungs_key, sitzungs_dict in data.items():
                cursor.execute(
                    """
                    INSERT INTO sitzungen (sitzungs_id, datum, start, ende)
                    VALUES (?, ?, ?, ?)
                """,
                    (
                        int(sitzungs_key),
                        sitzungs_dict["metadaten"]["datum"],
                        sitzungs_dict["metadaten"]["sitzungsbeginn"],
                        sitzungs_dict["metadaten"]["sitzungsende"],
                    ),
                )

                # iterate through the tagesordnungspunkte in each sitzung and add the foreign key
                for (
                    tagesordnungspunkt_key,
                    tagesordnungspunkt_dict,
                ) in sitzungs_dict["inhalt"].items():
                    cursor.execute(
                        """
                        INSERT INTO tagesordnungspunkte (name, sitzungs_id)
                        VALUES (?, ?)
                    """,
                        (tagesordnungspunkt_key, int(sitzungs_key)),
                    )
                    tagesordnungspunkt_id = cursor.lastrowid

                    # iterate through all reden in each tagesordnungspunkt and add the foreign key to it, the speaker and their role if they have one
                    for rede_key, rede_values in tagesordnungspunkt_dict.items():
                        # if "rolle" in rede_values["reference"]:
                        cursor.execute(
                            """
                            INSERT INTO reden (rede_id, text, redner_id, tagesordnungspunkt_id, rollen_id)
                            VALUES (?, ?, ?, ?, ?)
                        """,
                            (
                                rede_key,
                                "\n".join(rede_values["text"]),
                                rede_values["reference"]["redner"],
                                tagesordnungspunkt_id,
                                rede_values["reference"].get("rolle"),
                            ),
                        )
                        for kommentar_key, kommentar_values in rede_values.get(
                            "kommentare", {}
                        ).items():
                            cursor.execute(
                                """
                                INSERT INTO kommentare (kommentar_index, kommentator, fraktion, text, rede_id)
                                VALUES (?, ?, ?, ?, ?)
                            """,
                                (
                                    int(kommentar_key),
                                    kommentar_values["commentator"],
                                    kommentar_values["fraktion"],
                                    kommentar_values["text"],
                                    rede_key,
                                ),
                            )
            conn.commit()
