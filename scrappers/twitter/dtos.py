from dataclasses import dataclass
from typing import Dict, Any

@dataclass
class TweetDTO:
    tweet_id: str
    username: str
    handle: str
    text: str
    timestamp: str
    replies: str
    retweets: str
    likes: str
    views: str
    hashtag: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "tweet_id": self.tweet_id,
            "username": self.username,
            "handle": self.handle,
            "text": self.text,
            "timestamp": self.timestamp,
            "replies": self.replies,
            "retweets": self.retweets,
            "likes": self.likes,
            "views": self.views,
            "hashtag": self.hashtag
        }