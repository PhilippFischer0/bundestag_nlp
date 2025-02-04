import requests

from bs4 import BeautifulSoup
import os
import time
import re


def scrape_data() -> None:

    url = "https://www.bundestag.de/ajax/filterlist/de/services/opendata/866354-866354"

    payload = {
        "limit": 10,
        "FilterSet": "true",
        "offset": 0,
    }

    # get maximum number of XML Documents
    data_hits = int(
        BeautifulSoup(requests.get(url=url, params=payload)._content, "html.parser")
        .find("div", class_="meta-slider")
        .get("data-hits")
    )

    while payload["offset"] < data_hits:
        response = requests.get(url=url, params=payload)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, "html.parser")
            for link in soup.find_all("a", attrs={"title": re.compile("^XML")}):

                href = link.get("href")
                xml_response = requests.get(href)
                xml_file_name = href.split("/")[-1]

                with open(
                    os.path.join(os.getenv("XML_PATH"), xml_file_name), "wb"
                ) as xml_file:
                    xml_file.write(xml_response.content)

                time.sleep(5)

            payload["offset"] += 10
