import sqlite3, json


def extract_unique_redner(file_path):
    # recursively iterate through the json file to extract all unique redner dictionaries
    with open(file_path, "r", encoding="utf-8") as file:
        data = json.load(file)
    unique_redner = {}

    def find_redner(obj):
        if isinstance(obj, dict):
            if "redner" in obj:
                redner = obj["redner"]
                redner_id = redner["redner_id"]
                if redner_id not in unique_redner:
                    unique_redner[redner_id] = redner
            for key, value in obj.items():
                find_redner(value)
        elif isinstance(obj, list):
            for item in obj:
                find_redner(item)

    find_redner(data)
    # return all unique instances of redner in a list
    return list(unique_redner.values())


def extract_unique_rollen(unique_redner_list: json):
    # iterate through all unique speakers to extract all roles present
    unique_rollen = {}
    rollen_map = {}
    index_counter = 1

    for item in unique_redner_list:
        if "rolle" in item:
            unique_rollen.update({index_counter: item["rolle"]})
            rollen_map.update({item["rolle"]: index_counter})
            index_counter += 1
        else:
            continue

    # return the map so foreign keys can be set later on
    return unique_rollen, rollen_map


unique_redner_list = extract_unique_redner("data/out.json")

rollen, r_map = extract_unique_rollen(unique_redner_list)

# load file data
with open("data/out.json", "r") as file:
    data = json.load(file)
    # connect to the database and initialize cursor to execute SQL statements
    with sqlite3.connect("data/data.db") as conn:
        cursor = conn.cursor()

        # iterate through the roles and add them to the relevant table
        for key, rolle in rollen.items():
            cursor.execute(
                """
                INSERT INTO rollen (rollen_id, beschreibung)
                VALUES (?, ?)
            """,
                (int(key), rolle),
            )

        # iterate through unique_redner_list and add each item to the redner table
        for redner_item in unique_redner_list:
            # case 1 of 4 -> redner has a title and a role
            if "titel" in redner_item and "rolle" in redner_item:
                cursor.execute(
                    """
                    INSERT INTO redner (redner_id, titel, vorname, nachname)
                    VALUES (?, ?, ?, ?)
                """,
                    (
                        int(redner_item["redner_id"]),
                        redner_item["titel"],
                        redner_item["vorname"],
                        redner_item["nachname"],
                    ),
                )
            # case 2 of 4 -> redner has a title but no role
            elif "titel" in redner_item and not "rolle" in redner_item:
                cursor.execute(
                    """
                    INSERT INTO redner (redner_id, titel, vorname, nachname, fraktion)
                    VALUES (?, ?, ?, ?, ?)
                """,
                    (
                        int(redner_item["redner_id"]),
                        redner_item["titel"],
                        redner_item["vorname"],
                        redner_item["nachname"],
                        redner_item["fraktion"],
                    ),
                )
            # case 3 of 4 -> redner has a role but no title
            elif "rolle" in redner_item and not "titel" in redner_item:
                cursor.execute(
                    """
                    INSERT INTO redner (redner_id, vorname, nachname)
                    VALUES (?, ?, ?)
                """,
                    (
                        int(redner_item["redner_id"]),
                        redner_item["vorname"],
                        redner_item["nachname"],
                    ),
                )
            # case 4 of 4 -> redner has neither a title nor a role
            else:
                cursor.execute(
                    """
                    INSERT INTO redner (redner_id, vorname, nachname, fraktion)
                    VALUES (?, ?, ?, ?)
                """,
                    (
                        int(redner_item["redner_id"]),
                        redner_item["vorname"],
                        redner_item["nachname"],
                        redner_item["fraktion"],
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
                    if "rolle" in rede_values["redner"]:
                        cursor.execute(
                            """
                            INSERT INTO reden (rede_id, text, redner_id, tagesordnungspunkt_id, rollen_id)
                            VALUES (?, ?, ?, ?, ?)
                        """,
                            (
                                rede_key,
                                "\n".join(rede_values["text"]),
                                rede_values["redner"]["redner_id"],
                                tagesordnungspunkt_id,
                                # here the role map is used to get the corresponding foreign key
                                r_map[rede_values["redner"]["rolle"]],
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
                                rede_values["redner"]["redner_id"],
                                tagesordnungspunkt_id,
                            ),
                        )
                    # iterate through all comments made in a speech and add them to the database
                    for kommentar in rede_values["kommentare"]:
                        cursor.execute(
                            """
                            INSERT INTO kommentare (kommentar_index, text, rede_id)
                            VALUES (?, ?, ?)
                        """,
                            (
                                int(list(kommentar.keys())[0]),
                                list(kommentar.values())[0],
                                rede_key,
                            ),
                        )
        conn.commit()
