import os
import re
import time
from http import HTTPStatus

import requests
from bs4 import BeautifulSoup


def scrape_data(output_directory_path: str) -> None:

    url = "https://www.bundestag.de/ajax/filterlist/de/services/opendata/866354-866354"

    params = {
        "limit": 10,
        "FilterSet": "true",
        "offset": 0,
    }

    # get maximum number of XML Documents
    data_hits = int(
        BeautifulSoup(requests.get(url=url, params=params)._content, "html.parser")
        .find("div", class_="meta-slider")
        .get("data-hits")
    )

    while params["offset"] < data_hits:
        response = requests.get(url=url, params=params)
        if response.status_code == HTTPStatus.OK:
            soup = BeautifulSoup(response.content, "html.parser")
            for link in soup.find_all("a", attrs={"title": re.compile("^XML")}):

                href = link.get("href")
                xml_response = requests.get(href)
                xml_file_name = href.split("/")[-1]

                with open(
                    os.path.join(output_directory_path, xml_file_name), "wb"
                ) as xml_file:
                    xml_file.write(xml_response.content)

                time.sleep(5)

            params["offset"] += 10
