import tweepy
import config
import re


def filter_tweet(s) -> str:
    pattern = "@\S+|https?:\S+|http?:\S|[^A-Za-z0-9]+"
    tweet = re.sub(pattern, '', s)
    return tweet


class MyStreamListener(tweepy.Stream):
    def on_status(self, status):
        hashtags = []
        tweet = ""
        data = ""
        # row_header = ["created_at", "is_user_verified", "location", "hashtags", "tweet"]
        if status.retweeted is False:
            if len(status.entities['hashtags']) > 0:
                for i in range(0, len(status.entities['hashtags'])):
                    hashtag = status.entities['hashtags'][i]['text']
                    hashtags.append(status.entities['hashtags'][i]['text'])
                    data += hashtag+","

            if status.truncated is False:
                # print(filter_tweet(status.text))
                tweet = filter_tweet(status.text)
            else:
                pass
            print("-" * 40)
            created_at = status.created_at
            verified = str(status.user.verified)
            location = str(status.user.location)
            data += (verified + "|" + location + "|" + tweet)

            with open('data.txt', 'a') as f:
                f.write(data + "\n")

    def on_error(self, status_code):
        if status_code == 420:
            return False


stream = MyStreamListener(config.API_KEY,
                          config.SECRET_KEY,
                          config.ACCESS_TOKEN,
                          config.ACCESS_TOKEN_SECRET)

if __name__ == "__main__":
    stream.filter(track=["Python"], languages=["en"])
