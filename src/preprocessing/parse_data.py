import json
import os
import re
import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import Path


class PlenarprotokollXMLParser:
    def __init__(self):
        self.data = dict()
        self.redner = dict()
        self.rollen = dict()
        self.rollen_counter = 1

    # due to the xml-files containing invisible and ambiguous characters they have to be removed
    def remove_bad_chars(self, text: str) -> str:
        sub_map = {
            r"[\u00A0]": " ",
            r"[\u202F]": "",
            r"[\u2013]": "-",
            r"[\u201c\u201e]": "'",
        }

        for pattern, substitute in sub_map.items():
            text = re.compile(pattern=pattern).sub(substitute, text)

        return text

    def extract_spoken_comments(self, comment: str) -> list[dict]:
        pattern = re.compile(
            r"([A-Za-zäöüÄÖÜßğ\.\s]+) \[([A-Za-z0-9äöüÄÖÜß\s/]+)\]: (.+)"
        )
        segments = comment.split("-")
        extracted_info = []
        for segment in segments:
            matches = pattern.findall(segment)
            for match in matches:
                person, party, text = match
                text = text.rstrip(")")
                extracted_info.append(
                    {
                        "commentator": person.strip(),
                        "fraktion": party.strip(),
                        "text": text.strip(),
                    }
                )
        return extracted_info

    def get_xml_content(self, file_path: str) -> dict:
        tree = ET.parse(file_path)
        root = tree.getroot()

        # get the filename attributes from the root element and add as key in dictionary
        file_id = f"{root.attrib.get('wahlperiode')}{root.attrib.get('sitzung-nr')}"

        if file_id not in self.data:
            self.data[file_id] = {"metadaten": {}, "inhalt": {}}

        # get metadata of the sitzung and add the to the metadata dict
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

        # iterate through all tagesordnungspunkte in a sitzung
        for tagesordnungspunkt in sitzung.findall("tagesordnungspunkt"):
            tagesordnungspunkt_id = tagesordnungspunkt.attrib.get("top-id")

            tagesordnungspunkt_dict = {tagesordnungspunkt_id: {}}

            # iterate through all rede elements and add them to the corresponding tagesordnungspunkt
            for rede in tagesordnungspunkt.findall("rede"):
                # add two lists, one for the text of the speech the other for comments made by other politicians
                rede_dict = {"text": []}
                kommentar_dict = {"kommentare": {}}
                reference_dict = {"reference": {}}
                rede_id = rede.attrib.get("id")

                # initialize a comment counter which is later used as an index
                comment_counter = 0

                # iterate through all paragraphs of a speech
                for text_paragraph in rede:
                    # check if it is a relevant paragraph for the speech text, if so extend the text list with it
                    if not (
                        text_paragraph.attrib.get("klasse") == "redner"
                        or text_paragraph.tag == "kommentar"
                        and text_paragraph.text is not None
                    ):
                        # TODO: <name> Elemente + nachfolgende Rede aus Text parsen
                        rede_paragraph = self.remove_bad_chars(text_paragraph.text)
                        rede_dict["text"].append(rede_paragraph)

                    # else check if it is a comment, if so add it to the list of comments
                    elif text_paragraph.tag == "kommentar":
                        text_paragraph = self.remove_bad_chars(text_paragraph.text)
                        comments = self.extract_spoken_comments(text_paragraph)
                        for comment in comments:
                            comment_counter += 1
                            kommentar_dict["kommentare"][comment_counter] = comment

                if rede_id not in tagesordnungspunkt_dict[tagesordnungspunkt_id]:
                    tagesordnungspunkt_dict[tagesordnungspunkt_id][rede_id] = (
                        defaultdict(dict)
                    )

                tagesordnungspunkt_dict[tagesordnungspunkt_id][rede_id] = rede_dict

                if len(kommentar_dict["kommentare"]) > 0:
                    tagesordnungspunkt_dict[tagesordnungspunkt_id][rede_id].update(
                        kommentar_dict
                    )

                # extract the redner element from the relevant paragraph
                if rede.find("p").attrib.get("klasse") == "redner":
                    redner_paragraph = rede.find("p")
                    redner_element = redner_paragraph.find("redner")
                    # get the id from the speaker and add it to th relevant dictionary
                    redner_id = redner_element.attrib.get("id")
                    redner = {"redner_id": redner_id}
                    # find the name element, which holds the necessary metainformation
                    name = redner_element.find("name")

                    # iterate through the elements in the name element and add them to the redner dictionary
                    for element in name:
                        # check if the redner has a role and if so adds it to the dict
                        if element.tag == "rolle":
                            rollen_element = element.find("rolle_lang").text
                            rollen_element = re.compile(r"\s+").sub(" ", rollen_element)
                            if len(list(self.rollen.values())) == 0:
                                self.rollen["rollen"] = defaultdict()
                                self.rollen["map"] = defaultdict()
                            if rollen_element not in self.rollen["rollen"].values():
                                self.rollen["rollen"][
                                    self.rollen_counter
                                ] = rollen_element
                                self.rollen["map"][rollen_element] = str(
                                    self.rollen_counter
                                )
                                self.rollen_counter += 1
                        else:
                            redner[str(element.tag)] = self.remove_bad_chars(
                                element.text
                            )
                    if redner_id not in self.redner.keys():
                        self.redner[redner_id] = redner

                reference_dict["reference"]["redner"] = redner_id
                if name.find("rolle") is not None:
                    reference_dict["reference"]["rolle"] = self.rollen["map"][
                        rollen_element
                    ]

                tagesordnungspunkt_dict[tagesordnungspunkt_id][rede_id].update(
                    reference_dict
                )

            if (
                tagesordnungspunkt_id not in self.data[file_id]
                and len(list(tagesordnungspunkt_dict[tagesordnungspunkt_id].values()))
                > 0
            ):
                self.data[file_id]["inhalt"].update(tagesordnungspunkt_dict)

        return self.data, self.redner, self.rollen

    # iterate through directory to append the data from each file present into one json file
    def crawl_directory(
        self, input_directory_path: str, output_directory_path: str
    ) -> None:
        pathlist = sorted(Path(input_directory_path).rglob("*.xml"))

        for path in pathlist:
            data, redner, rollen = self.get_xml_content(path)
            self.data.update(data)
            self.redner.update(redner)
            self.rollen.update(rollen)

        with open(
            os.path.join(output_directory_path, "data.json"), "at", encoding="utf-8"
        ) as file:
            json.dump(self.data, file, indent=4, ensure_ascii=False)
        with open(
            os.path.join(output_directory_path, "redner.json"), "at", encoding="utf-8"
        ) as file:
            json.dump(self.redner, file, indent=4, ensure_ascii=False)
        with open(
            os.path.join(output_directory_path, "rollen.json"), "at", encoding="utf-8"
        ) as file:
            json.dump(self.rollen, file, indent=4, ensure_ascii=False)
