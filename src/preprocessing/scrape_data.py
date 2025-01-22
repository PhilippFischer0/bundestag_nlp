import xml.etree.ElementTree as ET

tree = ET.parse('../../data/xml/20202.xml')

root = tree.getroot()


for child in root:
    print(child)

sitzung = root.find("sitzungsverlauf")

for _child in sitzung:
    print(_child)

tagesordnung = sitzung.findall("tagesordnungspunkt")

for tagesordnungspunkt in tagesordnung:
    reden = tagesordnungspunkt.findall("rede")
    for i in range(len(reden)):
        paragraphs = reden[i].findall("p") 
        print(i)
        for paragraph in paragraphs:
            print(paragraph.text)