"""
Handles scrapping of data from twitter
"""
import random
import time
import json

import structlog

from typing import List, Tuple

from selenium_drivers.twitter import TwitterSeleniumHandler
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from concurrent.futures import ThreadPoolExecutor

from scrappers.twitter.dtos import TweetDTO
from settings import IMPORTANT_TWITTER_TAGS, MAX_TWITTER_PAGE_SCROLLS, ROOT_PATH, MAX_TWITTER_THREADS, SCRAPPED_DATA_FOLDER_NAME

logger = structlog.get_logger(__name__)

class TwitterDataScrapper:
    """
    Handles scrapping of data from twitter
    """

    
    OUTPUT_FOLDER = ROOT_PATH / SCRAPPED_DATA_FOLDER_NAME
    OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)

    MAX_SCROLLS = MAX_TWITTER_PAGE_SCROLLS

    def __init__(self) -> None:
        pass
    
    def scrape_hashtag(self, tag: str):
        """
        Scrape tweets for a given hashtag"""
        driver = TwitterSeleniumHandler().load_driver()

        # f=live, to pull in latest tweets
        driver.get(f"https://x.com/search?q=%23{tag}&f=live")
        time.sleep(4)  # wait for page to load

        tweets = []

        for i in range(self.MAX_SCROLLS):
            articles = driver.find_elements(By.XPATH, "//article")
            logger.info("Scrapping tweets",tag=tag,scroll_iteration=i+1, tweets_found=len(articles))
            for article in articles:
                try:
                    tweet = self.process_tweet(article, tag)
                except Exception:
                    continue
                if tweet:
                    tweets.append(tweet)
                else:
                    logger.warning("Tweet processing returned None", tag=tag)
            
            # random sleep to simulate random scrolling of page
            time.sleep(random.uniform(2.5, 4.5))
            # scroll down to load more tweets
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")

        logger.info("Finished scrapping tweets", tag=tag, total_tweets=len(tweets))
        self.dump_tweets(tweets, tag)
        driver.quit()
        return tweets
    
    def process(self):
        """
        Scrape tweets for all important hashtags in parallel
        """
        with ThreadPoolExecutor(max_workers=MAX_TWITTER_THREADS) as ex:
            results = ex.map(self.scrape_hashtag, IMPORTANT_TWITTER_TAGS)

        all_tweets = []
        for r in results:
            all_tweets.extend(r)
    
    def process_tweet(self, article: WebElement, hash_tag: str) -> TweetDTO:
        """
        Process a tweet article element and extract relevant data
        On twitter, each tweet is represented as an <article> element.

        Returns a dictionary with tweet data
        """
        # User info
        user_block = article.find_element(By.XPATH, ".//div[@data-testid='User-Name']")

        username = user_block.text.split("\n")[0]
        handle = user_block.text.split("\n")[1]

        # Text
        text = article.find_element(By.XPATH, ".//div[@data-testid='tweetText']").text

        # Time
        timestamp = article.find_element(By.XPATH, ".//time").get_attribute("datetime")

        def get_tweet_id(article) -> str:
            links = article.find_elements(By.XPATH, ".//a[contains(@href,'/status/')]")
            for l in links:
                href = l.get_attribute("href")
                if "/status/" in href:
                    return href.split("/status/")[1].split("?")[0]
            return ""

        # Metrics
        def get_metrics_from_group(article) -> Tuple[str, str, str, str]:
            # This is a trickier part for twitter, as the metrics are available
            # as aria-labels on buttons within a group role element.
            # there were no straightforward data-testid attributes for these metrics.
            # So we have to find the group element and then parse the aria-labels.
            metrics = {
                "replies": "0",
                "retweets": "0",
                "likes": "0",
                "views": "0"
            }

            groups = article.find_elements(By.XPATH, ".//*[@role='group']")
            for group in groups:
                buttons = group.find_elements(By.XPATH, ".//*[@aria-label]")
                for button in buttons:
                    label = button.get_attribute("aria-label").lower()
                    # eg: "0 Replies. Reply"
                    # similar aria labels for other metric buttons
                    if "reply" in label:
                        metrics["replies"] = label.split(" ")[0]
                    elif "repost" in label or "retweet" in label:
                        metrics["retweets"] = label.split(" ")[0]
                    elif "like" in label:
                        metrics["likes"] = label.split(" ")[0]
                    elif "view" in label:
                        metrics["views"] = label.split(" ")[0]

                return metrics["replies"], metrics["retweets"], metrics["likes"], metrics["views"]
            
            return "0", "0", "0", "0"    

        replies, retweets, likes, views = get_metrics_from_group(article)

        return TweetDTO(
            tweet_id=get_tweet_id(article),
            username=username,
            handle=handle,
            text=text,
            timestamp=timestamp or "",
            replies=replies,
            retweets=retweets,
            likes=likes,
            views=views,
            hashtag=hash_tag
        )
    
    def dump_tweets(self, tweets: List[TweetDTO], tag: str):
        with open(self.OUTPUT_FOLDER / f"{tag}.json", "w") as f:
            json.dump([tweet.to_dict() for tweet in tweets], f, indent=4)
