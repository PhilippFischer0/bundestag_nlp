from selenium import webdriver
from selenium.common.exceptions import WebDriverException

try:
    geckodriver_path = "/snap/bin/geckodriver"
    driver_service = webdriver.FirefoxService(executable_path=geckodriver_path)
    driver = webdriver.Firefox(service=driver_service)
    driver.get("https://www.bundestag.de/services/opendata")
    html = driver.page_source
    print(html[:100])
    driver.quit()
except WebDriverException as e:
    print(e)
