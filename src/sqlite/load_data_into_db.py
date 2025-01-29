import sqlite3, json


def extract_unique_redner(file_path):
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

    return list(unique_redner.values())


def extract_unique_rollen(unique_redner_list: json):
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

    return unique_rollen, rollen_map


unique_redner_list = extract_unique_redner("data/out.json")

rollen, r_map = extract_unique_rollen(unique_redner_list)

# with open("data/test.json", "w", encoding="utf-8") as file:
#     json.dump(unique_redner_list, file, indent=4, ensure_ascii=False)

with open("data/out.json", "r") as file:
    data = json.load(file)
    with sqlite3.connect("data/data.db") as conn:
        cursor = conn.cursor()
        for key, rolle in rollen.items():
            # print(rolle)
            cursor.execute(
                """
                INSERT INTO rollen (rollen_id, beschreibung)
                VALUES (?, ?)
            """,
                (int(key), rolle),
            )
        for redner_item in unique_redner_list:
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
                                r_map[rede_values["redner"]["rolle"]],
                            ),
                        )
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
