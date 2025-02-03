import requests
from requests import JSONDecodeError
from bs4 import BeautifulSoup
import os
import time, math


def scrape_data() -> None:

    base_url = "https://www.bundestag.de/ajax/filterlist/de/services/opendata/866354-866354?limit=10&noFilterSet=true&offset={offset}"

    offset = 0

    # get maximum number of XML Documents
    data_hits = int(
        BeautifulSoup(
            requests.get(base_url.format(offset=offset))._content, "html.parser"
        )
        .find("div")
        .get("data-hits")
    )

    while offset < data_hits:
        url = base_url.format(offset=offset)
        response = requests.get(url=url)
        try:
            soup = BeautifulSoup(response._content, "html.parser")
            for link in soup.find_all("a"):
                href = link.get("href")
                if href and href.endswith(".xml"):
                    xml_response = requests.get(href)
                    xml_file_name = href.split("/")[-1]
                    with open(
                        f"{os.getenv("XML_PATH")}/{xml_file_name}", "wb"
                    ) as xml_file:
                        xml_file.write(xml_response.content)
                time.sleep(5)
        except JSONDecodeError as e:
            print(e)

        offset += 10
