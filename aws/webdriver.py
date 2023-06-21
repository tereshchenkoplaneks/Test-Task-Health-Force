""" Scalable webdriver interface using selenium and requests simultaneously.
Execute boilerplate initialisation, closing, logging, user-agent and cookies-swapping automatically.
just change the backend when needed during your calls to the get method :
```python
webdriver.get(url, backend="selenium")
```
"""

import os
from urllib.parse import urlparse

import requests
import selenium
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.remote import webelement

from common import get_random_user_agent
from logger import logger


class WebDriver:
    def __init__(
        self, path_dir_output: str, path_exec_firefox: str, headless: bool = True
    ):
        self.path_dir_output = path_dir_output
        self.user_agent = get_random_user_agent()
        self.current_url = None

        self.backend = {
            "selenium": self._init_selenium(
                path_dir_output=path_dir_output,
                path_exec_firefox=path_exec_firefox,
                headless=headless,
            ),
            "requests": self._init_requests(),
        }

    def post(self, url: str, payload: dict):
        logger.debug(f"POST from requests into {url} with payload : {payload}")
        return self.backend["requests"].post(url=url, data=payload)

    def get(self, url: str, backend: str = "requests"):
        if backend == "selenium":
            return self._get_selenium(url=url)
        elif backend == "requests":
            return self._get_requests(url=url)
        else:
            raise ValueError(f'Unknown backend "{backend}"')

    def _get_selenium(self, url: str):
        logger.debug(f"GET from selenium into {url}")
        self.backend["selenium"].get(url)
        self._cookies_selenium_to_requests()
        self.current_url = self.backend["selenium"].current_url

    def _get_requests(self, url: str):
        logger.debug(f"GET from requests into {url}")

        headers = {
            "User-Agent": self.user_agent,
        }
        result = self.backend["requests"].get(url, headers=headers)

        self._cookies_requests_to_selenium(fetched_url=url)
        self.current_url = url
        return result

    def _init_selenium(
        self, path_dir_output: str, path_exec_firefox: str, headless: bool
    ):
        # Setup options & parameters
        options = Options()
        if headless:
            options.add_argument("-headless")

        # Needed to set the default download location
        options.set_preference("browser.download.folderList", 2)
        options.set_preference("browser.download.manager.showWhenStarting", False)
        options.set_preference("browser.download.dir", path_dir_output)
        options.set_preference(
            "browser.helperApps.neverAsk.saveToDisk", "application/x-gzip"
        )
        os.makedirs(path_dir_output, exist_ok=True)

        # Set the default user agent
        profile = webdriver.FirefoxProfile()
        profile.set_preference("general.useragent.override", self.user_agent)

        # Disable geckodriver logs
        service = Service(log_path=os.path.devnull)

        return webdriver.Firefox(
            options=options,
            firefox_profile=profile,
            service=service,
        )

    def _init_requests(self):
        return requests.Session()

    def _cookies_requests_to_selenium(self, fetched_url: str):
        cookies = self.backend["requests"].cookies.get_dict()

        # Extract the domain of the URL requested by requests
        fetched_domain = urlparse(fetched_url).netloc

        # Find if the domain has already been requested by the selenium driver
        for cookie in self.backend["selenium"].get_cookies():
            if cookie["domain"] == fetched_domain:
                break
        else:
            # We need to fetch the domain once by selenium for the cookies to be registred
            self.backend["selenium"].get(url=f"https://{fetched_domain}")

        # Replace cookies
        for cookie_name, cookie_value in cookies.items():
            self.backend["selenium"].add_cookie(
                ({"name": cookie_name, "value": cookie_value})
            )

    def _cookies_selenium_to_requests(self):
        cookies = self.backend["selenium"].get_cookies()
        for cookie in cookies:
            self.backend["requests"].cookies.set(cookie["name"], cookie["value"])

    def click_js(self, element: selenium.webdriver.remote.webelement.WebElement):
        return self.backend["selenium"].execute_script("arguments[0].click();", element)

    def find_element(self, *args, **kwargs):
        return self.backend["selenium"].find_element(*args, **kwargs)

    def find_elements(self, *args, **kwargs):
        return self.backend["selenium"].find_elements(*args, **kwargs)

    def quit(self):
        logger.debug("Closing webdriver")
        self.backend["selenium"].quit()
