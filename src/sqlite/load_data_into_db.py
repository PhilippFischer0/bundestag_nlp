import sqlite3, json
import os


def load_data_into_db(json_directory_path: str, database_path: str) -> None:
    # load file data
    with open(os.path.join(json_directory_path, "data.json"), "r") as file:
        data = json.load(file)
    with open(os.path.join(json_directory_path, "redner.json"), "r") as file:
        redner = json.load(file)
    with open(os.path.join(json_directory_path, "rollen.json"), "r") as file:
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
                # case 1 of 4 -> redner has a title and a fraktion
                if "titel" in redner_value and "fraktion" in redner_value:
                    cursor.execute(
                        """
                        INSERT INTO redner (redner_id, titel, vorname, nachname, fraktion)
                        VALUES (?, ?, ?, ?, ?)
                    """,
                        (
                            int(redner_key),
                            redner_value["titel"],
                            redner_value["vorname"],
                            redner_value["nachname"],
                            redner_value["fraktion"],
                        ),
                    )
                # case 2 of 4 -> redner has a title but no fraktion
                elif "titel" in redner_value and not "fraktion" in redner_value:
                    cursor.execute(
                        """
                        INSERT INTO redner (redner_id, titel, vorname, nachname)
                        VALUES (?, ?, ?, ?)
                    """,
                        (
                            int(redner_key),
                            redner_value["titel"],
                            redner_value["vorname"],
                            redner_value["nachname"],
                        ),
                    )
                # case 3 of 4 -> redner has a fraktion but no title
                elif "fraktion" in redner_value and not "titel" in redner_value:
                    cursor.execute(
                        """
                        INSERT INTO redner (redner_id, vorname, nachname, fraktion)
                        VALUES (?, ?, ?, ?)
                    """,
                        (
                            int(redner_key),
                            redner_value["vorname"],
                            redner_value["nachname"],
                            redner_value["fraktion"],
                        ),
                    )
                # case 4 of 4 -> redner has neither a title nor a fraktion
                else:
                    cursor.execute(
                        """
                        INSERT INTO redner (redner_id, vorname, nachname)
                        VALUES (?, ?, ?)
                    """,
                        (
                            int(redner_key),
                            redner_value["vorname"],
                            redner_value["nachname"],
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
                        if "rolle" in rede_values["reference"]:
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
                                    rede_values["reference"]["rolle"],
                                ),
                            )
                        # in case the speaker doesn't have a role
                        else:
                            cursor.execute(
                                """
                                INSERT INTO reden (rede_id, text, redner_id, tagesordnungspunkt_id)
                                VALUES (?, ?, ?, ?)
                            """,
                                (
                                    rede_key,
                                    "\n".join(rede_values["text"]),
                                    int(rede_values["reference"]["redner"]),
                                    tagesordnungspunkt_id,
                                ),
                            )
                        # iterate through all comments made in a speech and add them to the database
                        for kommentar in rede_values.get("kommentare", []):
                            kommentar_key = int(list(kommentar.keys())[0])
                            kommentar_values = kommentar[str(kommentar_key)]
                            cursor.execute(
                                """
                                INSERT INTO kommentare (kommentar_index, kommentator, fraktion, text, rede_id)
                                VALUES (?, ?, ?, ?, ?)
                            """,
                                (
                                    kommentar_key,
                                    kommentar_values["commentator"],
                                    kommentar_values["fraktion"],
                                    kommentar_values["text"],
                                    rede_key,
                                ),
                            )
            conn.commit()
