import re
import json

from typing import List

import structlog
import pandas as pd


from settings import (
    CLEANED_DATA_FOLDER_NAME,
    CLEANED_TWITTER_FILE_NAME, 
    ROOT_PATH, 
    SCRAPPED_DATA_FOLDER_NAME,
    IMPORTANT_TWITTER_TAGS
)

logger = structlog.get_logger(__name__)


class DataCleaner:

    CLEANED_DATA_PATH = ROOT_PATH / CLEANED_DATA_FOLDER_NAME
    CLEANED_DATA_PATH.mkdir(parents=True, exist_ok=True)

    def combine_segregated_data(self) -> pd.DataFrame:
        data = []

        SOURCE_FOLDER = ROOT_PATH / SCRAPPED_DATA_FOLDER_NAME

        for file in SOURCE_FOLDER.glob("*.json"):
            hashtag = file.stem   # banknifty, nifty50, etc

            with open(file, encoding="utf-8") as f:
                tweets = json.load(f)

            for t in tweets:
                t["hashtag"] = hashtag
                data.append(t)

        logger.info("Combining scrapped twitter data", total_records=len(data))
        df = pd.DataFrame(data)
        return df
    

    @staticmethod
    def clean_text(string: str) -> str:
        url_re = re.compile(r"http\S+|www\S+")
        multi_space = re.compile(r"\s+")

        if not isinstance(string, str):
            return ""
        string = string.lower()
        string = url_re.sub("", string)
        string = multi_space.sub(" ", string)
        return string.strip()
    
    @staticmethod
    def extract_hashtags(string: str) -> List[str]:
        hashtag_re = re.compile(r"#(\w+)")

        if not isinstance(string, str):
            return []
        # retain only those hashtags which are important
        # for analysis
        return [
            h.lower() for h in hashtag_re.findall(string)
            if h.lower() in IMPORTANT_TWITTER_TAGS
        ]
    
    @staticmethod
    def normalize_number(val):
        if val is None or val == "":
            return 0
        s = str(val).replace(",", "").upper()
        try:
            if s.endswith("K"):
                return int(float(s[:-1]) * 1_000)
            if s.endswith("M"):
                return int(float(s[:-1]) * 1_000_000)
            return int(float(s))
        except:
            return 0

    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Data cleaning process started", initial_rows=len(df))
        # -----------------------------
        # 1. Drop broken rows
        # -----------------------------
        df = df.dropna(subset=["text", "timestamp"])

        # -----------------------------
        # 2. Text cleaning
        # -----------------------------
        df["clean_text"] = df["text"].apply(self.clean_text)
        df["hashtags"] = df["text"].apply(self.extract_hashtags)

        # -----------------------------
        # 3. Normalize metrics
        # -----------------------------
        for col in ["replies", "retweets", "likes", "views"]:
            if col in df.columns:
                df[col] = df[col].apply(self.normalize_number)
            else:
                df[col] = 0

        # -----------------------------
        # 4. Deduplicate
        # -----------------------------
        df = df.drop_duplicates(subset=["tweet_id"])

        # -----------------------------
        # 5. Time normalization
        # -----------------------------
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        df = df.dropna(subset=["timestamp"])
        df = df.sort_values("timestamp")

        logger.info(
            "Rows after cleanup",
            rows=len(df)
        )
        return df


    def process(self):
        logger.info("Starting data cleaning process for twitter data")
        merged_df = self.combine_segregated_data()
        cleaned_df = self.clean_data(merged_df)
        cleaned_df.to_parquet(self.CLEANED_DATA_PATH / CLEANED_TWITTER_FILE_NAME, engine="pyarrow", compression="snappy")