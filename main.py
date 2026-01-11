from analysis.twitter.analyse import TwitterAnalyser
from cleaning.twitter import DataCleaner
from scrappers.twitter import TwitterDataScrapper

pipeline = [
    TwitterDataScrapper().process(),
    DataCleaner().process(),
    TwitterAnalyser().process(),
]