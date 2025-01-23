import xml.etree.ElementTree as ET
import json
from pathlib import Path


class XMLParser:
    def __init__(self):
        self.data = dict()

    def get_xml_content(self, file: str) -> dict:
        tree = ET.parse(file)
        root = tree.getroot()
        file_id = f"{root.attrib.get('wahlperiode')}{root.attrib.get('sitzung-nr')}"

        if file_id not in self.data:
            self.data[file_id] = []

        sitzung = root.find("sitzungsverlauf")

        for tagesordnungspunkt in sitzung.findall("tagesordnungspunkt"):
            tagesordnungspunkt_id = tagesordnungspunkt.attrib.get("top-id")

            if tagesordnungspunkt_id not in self.data[file_id]:
                self.data[file_id].append({tagesordnungspunkt_id: []})

            for rede in tagesordnungspunkt.findall("rede"):
                rede_id = rede.attrib.get("id")
                self.data[file_id][-1][tagesordnungspunkt_id].append({rede_id: []})
                for text_paragraph in rede:
                    if text_paragraph.attrib.get("klasse") == "redner":
                        redner_element = text_paragraph.find("redner")
                        if (
                            redner_element is not None
                            and redner_element.tail is not None
                        ):
                            text_paragraph = redner_element.tail.strip()
                            rede_paragraph = {redner_element.tag: text_paragraph}
                    else:
                        rede_paragraph = {text_paragraph.tag: text_paragraph.text}
                    self.data[file_id][-1][tagesordnungspunkt_id][-1][rede_id].append(
                        rede_paragraph
                    )

        return self.data

    def crawl_directory(self, dir_path: str, out_path: str) -> None:
        pathlist = sorted(Path(dir_path).rglob("*.xml"))

        for path in pathlist:
            self.data.update(self.get_xml_content(path))

        with open(out_path, "a") as file:
            json.dump(self.data, file, indent=4, ensure_ascii=False)


test = XMLParser()

test.crawl_directory("data/xml/", "data/out.json")
