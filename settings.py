from pathlib import Path


# TWITTER DATA SCRAPPING CONFIGS
IMPORTANT_TWITTER_TAGS = [
    "banknifty", 
    "nifty50", 
    "sensex", 
    "intraday",
]

ROOT_PATH = Path(__file__).parent

# Increase concurrency as needed, to speed up scrapping
# Twitter thinks the same user has opened multiple browser tabs
# since authentication cookies are shared across threads
MAX_TWITTER_THREADS = 2
# number of times to scroll the twitter page to load more tweets
MAX_TWITTER_PAGE_SCROLLS = 50


SCRAPPED_DATA_FOLDER_NAME = "data/scrapped_data"
CLEANED_DATA_FOLDER_NAME = "data/cleaned_data"
CLEANED_TWITTER_FILE_NAME = "cleaned_twitter_data.parquet"
TWITTER_TIME_SERIES_OUTPUT_FILE_NAME = "twitter_time_series.parquet"

PLOTS_FOLDER_NAME = "plots"

TWITTER_AUTH_COOKIES_FILE = "twitter_cookies.json"