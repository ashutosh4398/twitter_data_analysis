from typing import List, Dict, Any
from selenium import webdriver

class SeleniumHandler():
    """
    Base class for Selenium Handlers
    """
    def load_auth_cookies(self) -> List[Dict[str, Any]]:
        raise NotImplementedError()
    
    def load_options(self) -> webdriver.ChromeOptions:
        raise NotImplementedError()

    def load_driver(self) -> webdriver.Remote:
        raise NotImplementedError()
    
    
