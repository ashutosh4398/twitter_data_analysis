
import os
import json

import structlog

from typing import Any, Dict, List

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

from settings import ROOT_PATH, TWITTER_AUTH_COOKIES_FILE

logger = structlog.get_logger(__name__)

class TwitterSeleniumHandler:
    AUTH_COOKIES_PATH = ROOT_PATH / TWITTER_AUTH_COOKIES_FILE

    def load_auth_cookies(self) -> List[Dict[str, Any]]:
        """
        Load and return authentication cookies for a logged-in x.com user from twitter_cookies.json.

        Notes:
        - The file should contain cookies exported from the browser (e.g. using a "Get Cookies" extension).
        - Cookies are sensitive (session tokens). Do not commit this file to source control.
        - Supports either a dict mapping cookie-name->value or a list of cookie objects
        (each with 'name' and 'value' keys), and normalizes to a dict
        """
        with open(self.AUTH_COOKIES_PATH) as f:
            cookies = json.load(f)
        
        def format_cookie(_cookie: Dict[str, Any]) -> Dict[str, Any]:
            cookie = {
                "name": _cookie["name"],
                "value": _cookie["value"],
                "domain": _cookie.get("domain", ".x.com"),
                "path": _cookie.get("path", "/"),
            }
            if "expirationDate" in _cookie:
                try:
                    cookie["expiry"] = int(_cookie["expirationDate"])
                except:
                    pass
            
            return cookie
        
        return list(map(format_cookie, cookies))
    
    def load_options(self) -> Options:
        options: Options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--disable-blink-features=AutomationControlled")
        return options

    def load_driver(self) -> webdriver.Remote:
        options = self.load_options()
        auth_cookies: List[Dict[str, Any]] = self.load_auth_cookies()
        SELENIUM_REMOTE_URL = os.environ["SELENIUM_REMOTE_URL"]
        driver = webdriver.Remote(
            command_executor=SELENIUM_REMOTE_URL,
            options=options
        )
        
        # --- load the main page and set zoom level ---
        # this zoom level helps load more tweets in one scroll
        # reducing number of scrolls needed
        driver.get("https://x.com")
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        driver.execute_script("""
            document.body.style.zoom = '0.6';
            document.body.style.transformOrigin = '0 0';
        """)
        driver.set_window_size(1200, 4000)

        for cookie in auth_cookies:
            try:
                driver.add_cookie(cookie)
            except:
                pass

        logger.info("Selenium driver loaded with auth cookies")
        return driver