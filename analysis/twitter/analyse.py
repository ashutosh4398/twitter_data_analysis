import structlog
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from settings import CLEANED_DATA_FOLDER_NAME, CLEANED_TWITTER_FILE_NAME, IMPORTANT_TWITTER_TAGS, PLOTS_FOLDER_NAME, ROOT_PATH, SCRAPPED_DATA_FOLDER_NAME, TWITTER_TIME_SERIES_OUTPUT_FILE_NAME


logger = structlog.get_logger(__file__)

class TwitterAnalyser:
    SOURCE_DATA = ROOT_PATH / CLEANED_DATA_FOLDER_NAME / CLEANED_TWITTER_FILE_NAME

    def load_cleaned_data(self):
        df = pd.read_parquet(self.SOURCE_DATA)
        logger.info("Loaded cleaned twitter data", rows=len(df))
        return df

    def generate_sentiment(self, df: pd.DataFrame) -> pd.DataFrame:
        analyzer = SentimentIntensityAnalyzer()
        df["sentiment"] = df["clean_text"].apply(
            lambda t: analyzer.polarity_scores(t)["compound"]
        )
        logger.info("Generated sentiment scores for tweets")
        return df


    def generate_weights(self, df: pd.DataFrame) -> pd.DataFrame:
        df["engagement"] = (
            df["views"]
            + 2 * df["likes"]
            + 3 * df["retweets"]
            + df["replies"]
        )
        df["impact"] = np.log1p(df["engagement"])
        df["weighted_sentiment"] = df["sentiment"] * df["impact"]
        return df

    def aggregate(self, df: pd.DataFrame) -> pd.DataFrame:
        # Each tweet contributes to all its hashtags
        df = df.explode("hashtags")
        df = df.dropna(subset=["hashtags"])

        # Bucket by hour
        df["hour"] = df["timestamp"].dt.floor("1H")

        signals = (
            df.groupby(["hashtags", "hour"])
            .agg(
                tweet_count=("clean_text", "count"),
                avg_sentiment=("sentiment", "mean"),
                weighted_sentiment=("weighted_sentiment", "mean"),
                sentiment_volatility=("sentiment", "std"),
            )
            .reset_index()
            .rename(columns={"hashtags": "hashtag"})
        )

        return signals
    
    def save_final_signals(self, signals: pd.DataFrame):
        output_file = ROOT_PATH / CLEANED_DATA_FOLDER_NAME / TWITTER_TIME_SERIES_OUTPUT_FILE_NAME
        signals.to_parquet(output_file)
        logger.info("Saved final aggregated signals", path=str(output_file), rows=len(signals))
    
    def visualize(self, signals: pd.DataFrame):
        targets = IMPORTANT_TWITTER_TAGS
        (ROOT_PATH / PLOTS_FOLDER_NAME).mkdir(parents=True, exist_ok=True)
        
        logger.info("Visualizing sentiment signals", targets=targets)
        df = signals[signals["hashtag"].isin(targets)]

        # Convert time
        df["hour"] = pd.to_datetime(df["hour"])

        # Downsample if huge (memory safe)
        if len(df) > 50_000:
            df = df.sample(50_000)
        
        # Rolling smooth for visualization
        df = df.sort_values("hour")
        df["smooth_signal"] = (
            df.groupby("hashtag")["weighted_sentiment"]
            .transform(lambda x: x.rolling(3, min_periods=1).mean())
        )

        # -----------------------------
        # Plot sentiment over time
        # -----------------------------
        for tag in targets:
            subset = df[df["hashtag"] == tag]

            if subset.empty:
                continue

            plt.figure(figsize=(10, 4))
            plt.plot(subset["hour"], subset["smooth_signal"])
            plt.title(f"{tag.upper()} Market Sentiment")
            plt.xlabel("Time")
            plt.ylabel("Weighted Sentiment")
            plt.tight_layout()
            plt.savefig(f"{ROOT_PATH / PLOTS_FOLDER_NAME}/{tag}_sentiment.png")
            plt.close()
        
        logger.info("Saved sentiment plots in /plots")





    def process(self):
        df = self.load_cleaned_data()
        df = self.generate_sentiment(df)
        df = self.generate_weights(df)
        signals = self.aggregate(df)
        self.save_final_signals(signals)
        self.visualize(signals)