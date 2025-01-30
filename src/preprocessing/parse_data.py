import xml.etree.ElementTree as ET
import json, re
from pathlib import Path


class PlenarprotokollXMLParser:
    def __init__(self):
        self.data = dict()

    # due to the xml-files containing invisible and ambiguous characters they have to be removed
    def remove_bad_chars(self, text):
        # TODO: improve this so ambiguous characters are gone, removes some spacing characters that should stay
        invisible_spaces_pattern = re.compile(r"[\u00A0]")
        invisible_pattern = re.compile(r"[\u202F]")
        bad_dashes_pattern = re.compile(r"[\u2013]")
        bad_upper_quotation_pattern = re.compile(r"[\u201c]")
        bad_lower_quotation_pattern = re.compile(r"[\u201e]")

        cleaned_text = invisible_spaces_pattern.sub(" ", text)
        cleaned_text = invisible_pattern.sub("", cleaned_text)
        cleaned_text = bad_dashes_pattern.sub("-", cleaned_text)
        cleaned_text = bad_upper_quotation_pattern.sub("'", cleaned_text)
        cleaned_text = bad_lower_quotation_pattern.sub("'", cleaned_text)
        return cleaned_text

    def get_xml_content(self, file: str) -> dict:
        tree = ET.parse(file)
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

        # iterate through all tagesordnungspunkte in a sitzung and add the to inhalt
        for tagesordnungspunkt in sitzung.findall("tagesordnungspunkt"):
            tagesordnungspunkt_id = tagesordnungspunkt.attrib.get("top-id")

            if tagesordnungspunkt_id not in self.data[file_id]:
                self.data[file_id]["inhalt"].update({tagesordnungspunkt_id: {}})

            # iterate through all rede elements and add them to the corresponding tagesordnungspunkt
            for rede in tagesordnungspunkt.findall("rede"):
                rede_id = rede.attrib.get("id")
                self.data[file_id]["inhalt"][tagesordnungspunkt_id].update(
                    {rede_id: {}}
                )

                # add two lists, one for the text of the speech the other for comments made by other politicians
                self.data[file_id]["inhalt"][tagesordnungspunkt_id][rede_id].update(
                    {"text": [], "kommentare": []}
                )
                # initialize a comment counter which is later used as an index
                comment_counter = 0

                # iterate through all paragraphs of a speech
                for text_paragraph in rede:
                    # check if it is a relevant paragraph for the speech text, if so extend the text list with it
                    if not (
                        text_paragraph.attrib.get("klasse") == "redner"
                        or text_paragraph.tag == "kommentar"
                    ):
                        rede_paragraph = [self.remove_bad_chars(text_paragraph.text)]
                        self.data[file_id]["inhalt"][tagesordnungspunkt_id][rede_id][
                            "text"
                        ].extend(rede_paragraph)

                    # else check if it is a comment, if so add it to the list of comments
                    elif text_paragraph.tag == "kommentar":
                        comment_counter += 1
                        self.data[file_id]["inhalt"][tagesordnungspunkt_id][rede_id][
                            "kommentare"
                        ].append(
                            {
                                comment_counter: self.remove_bad_chars(
                                    text_paragraph.text
                                )
                            }
                        )

                # extract the redner element from the relevant paragraph
                if rede.find("p").attrib.get("klasse") == "redner":
                    redner_paragraph = rede.find("p")
                    redner_element = redner_paragraph.find("redner")
                    # find the name element, which holds the necessary metainformation
                    name = redner_element.find("name")
                    # get the id from the speaker and add it to th relevant dictionary
                    redner_id = redner_element.attrib.get("id")
                    redner = {"redner_id": redner_id}

                    # iterate through the elements in the name element and add them to the redner dictionary
                    for element in name:
                        redner[str(element.tag)] = (
                            self.remove_bad_chars(element.text)
                            if element.text is not None
                            # check if the redner has a role and if so adds it to the dict
                            else self.remove_bad_chars(element.find("rolle_lang").text)
                        )
                    self.data[file_id]["inhalt"][tagesordnungspunkt_id][rede_id].update(
                        {"redner": redner}
                    )

        # some tagesordnungspunkte don't contain speeches so they are removed here
        keys_to_remove = []
        for tagesordnungspunkt_key, tagesordnungspunkt_values in self.data[file_id][
            "inhalt"
        ].items():
            if len(list(tagesordnungspunkt_values)) == 0:
                keys_to_remove.append(tagesordnungspunkt_key)

        for key in keys_to_remove:
            self.data[file_id]["inhalt"].pop(key)

        return self.data

    # iterate through directory to append the data from each file present into one json file
    def crawl_directory(self, dir_path: str, out_path: str) -> None:
        pathlist = sorted(Path(dir_path).rglob("*.xml"))

        for path in pathlist:
            self.data.update(self.get_xml_content(path))

        with open(out_path, "a") as file:
            json.dump(self.data, file, indent=4, ensure_ascii=False)


test = PlenarprotokollXMLParser()

test.crawl_directory("data/xml/", "data/out.json")
