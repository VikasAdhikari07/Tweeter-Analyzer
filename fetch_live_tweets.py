import tweepy
import config
import time


class MyStreamListener(tweepy.Stream):
    def on_status(self, status):
        if status.retweeted is False:
            # print(status)
            print(status.created_at, status.user.verified, status.user.location)
            print(status.quoted_status)
            time.sleep(10)

    def on_error(self, status_code):
        if status_code == 420:
            return False


stream = MyStreamListener(config.API_KEY,
                          config.SECRET_KEY,
                          config.ACCESS_TOKEN,
                          config.ACCESS_TOKEN_SECRET)

stream.filter(track=["Python"], languages=["en"])