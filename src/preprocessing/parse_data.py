import xml.etree.ElementTree as ET
import json, re
from pathlib import Path


class PlenarprotokollXMLParser:
    def __init__(self):
        self.data = dict()

    def remove_invisible_chars(self, text):
        invisible_chars_pattern = re.compile(r"[\u200B-\u200D\uFEFF\u00A0]")
        cleaned_text = invisible_chars_pattern.sub("", text)

        return cleaned_text

    def get_xml_content(self, file: str) -> dict:
        tree = ET.parse(file)
        root = tree.getroot()
        file_id = f"{root.attrib.get('wahlperiode')}{root.attrib.get('sitzung-nr')}"

        if file_id not in self.data:
            self.data[file_id] = {"metadaten": {}, "inhalt": {}}

        veranstaltungsdaten = root.find("vorspann/kopfdaten/veranstaltungsdaten")
        date = veranstaltungsdaten.find("datum").attrib.get("date")

        sitzung = root.find("sitzungsverlauf")
        sitzungsbeginn = sitzung.find("sitzungsbeginn").attrib.get(
            "sitzung-start-uhrzeit"
        )
        sitzungsende = sitzung.find("sitzungsende").attrib.get("sitzung-ende-uhrzeit")
        self.data[file_id]["metadaten"] = {
            "datum": date,
            "sitzungsbeginn": sitzungsbeginn,
            "sitzungsende": sitzungsende,
        }

        for tagesordnungspunkt in sitzung.findall("tagesordnungspunkt"):
            tagesordnungspunkt_id = tagesordnungspunkt.attrib.get("top-id")

            if tagesordnungspunkt_id not in self.data[file_id]:
                self.data[file_id]["inhalt"].update({tagesordnungspunkt_id: {}})

            # self.data[file_id][-1][tagesordnungspunkt_id].append({"thema": []})
            # for tagesordnungspunkt_paragraph in tagesordnungspunkt.findall("p"):
            #     if tagesordnungspunkt_paragraph.text is not None:
            #         self.data[file_id][-1][tagesordnungspunkt_id][-1]["thema"].extend(
            #             [self.remove_invisible_chars(tagesordnungspunkt_paragraph.text)]
            #         )

            for rede in tagesordnungspunkt.findall("rede"):
                rede_id = rede.attrib.get("id")
                self.data[file_id]["inhalt"][tagesordnungspunkt_id].update(
                    {rede_id: {}}
                )

                self.data[file_id]["inhalt"][tagesordnungspunkt_id][rede_id].update(
                    {"text": [], "kommentare": []}
                )
                rede_paragraph = []
                comment_counter = 0

                for text_paragraph in rede:
                    if not (
                        text_paragraph.attrib.get("klasse") == "redner"
                        or text_paragraph.tag == "kommentar"
                    ):
                        rede_paragraph = [
                            self.remove_invisible_chars(text_paragraph.text)
                        ]
                        self.data[file_id]["inhalt"][tagesordnungspunkt_id][rede_id][
                            "text"
                        ].extend(rede_paragraph)

                    elif text_paragraph.tag == "kommentar":
                        comment_counter += 1
                        self.data[file_id]["inhalt"][tagesordnungspunkt_id][rede_id][
                            "kommentare"
                        ].append(
                            {
                                comment_counter: self.remove_invisible_chars(
                                    text_paragraph.text
                                )
                            }
                        )

                if rede.find("p").attrib.get("klasse") == "redner":
                    redner_paragraph = rede.find("p")
                    redner_element = redner_paragraph.find("redner")
                    name = redner_element.find("name")
                    redner_id = redner_element.attrib.get("id")
                    redner = {"redner_id": redner_id}

                    for element in name:
                        redner[str(element.tag)] = (
                            self.remove_invisible_chars(element.text)
                            if element.text is not None
                            else self.remove_invisible_chars(
                                element.find("rolle_lang").text
                            )
                        )
                    self.data[file_id]["inhalt"][tagesordnungspunkt_id][rede_id].update(
                        {"redner": redner}
                    )

        keys_to_remove = []
        for tagesordnungspunkt_key, tagesordnungspunkt_values in self.data[file_id][
            "inhalt"
        ].items():
            if len(list(tagesordnungspunkt_values)) == 0:
                keys_to_remove.append(tagesordnungspunkt_key)

        for key in keys_to_remove:
            self.data[file_id]["inhalt"].pop(key)

        return self.data

    def crawl_directory(self, dir_path: str, out_path: str) -> None:
        pathlist = sorted(Path(dir_path).rglob("*.xml"))

        for path in pathlist:
            self.data.update(self.get_xml_content(path))

        with open(out_path, "a") as file:
            json.dump(self.data, file, indent=4, ensure_ascii=False)


test = PlenarprotokollXMLParser()

test.crawl_directory("data/xml/", "data/out.json")
