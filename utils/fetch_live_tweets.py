import tweepy
import config
import settings
import mysql.connector
import re


def clean_tweet(tweet) -> str:
    """
    Use simmple regex statements to clean tweet text by removing links and special characters
    """
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t]) \
                                |(\w+:\/\/\S+)", " ", tweet).split())

def de_emojify(text):
    """
    Ship all non-Ascii characters to remove all the emoji characters
    """
    if text:
        return text.encode("ascii", "ignore").decode("ascii")
    else:
        return None


class MyStreamListener(tweepy.Stream):
    def on_status(self, status):
        """
        Extract info from tweets
        """
        hashtags = []
        tweet = ""
        data = ""
        if status.retweeted:
            # Avoid retweeted info and only original tweets will be reveived
            return True

        # Extract hashtags
        if len(status.entities['hashtags']) > 0:
            for i in range(0, len(status.entities['hashtags'])):
                hashtag = status.entities['hashtags'][i]['text']
                hashtags.append(status.entities['hashtags'][i]['text'])
                data += hashtag+","

        # Extract Attributes from each tweet
        if status.truncated is False:
            tweet = clean_tweet(status.text)
        else:
            pass
        user_created_at = status.user.created_at
        is_user_verified = str(status.user.verified)
        user_location = de_emojify(status.user.location)
        user_description = de_emojify(status.user.description)
        user_follower_count = status.user.followers_count
        latitude = None
        longitude = None
        tweet_location = str(status.user.location)

        if status.coordinates:
            longitude = status.coordinates['coordinates'][0]
            latitude = status.coordinates['coordinates'][1]

        retweet_count = status.retweet_count
        favorite_count = status.favorite_count

        print(tweet)
        print("Long: {}, Lati: {}".format(longitude, latitude))

        # store all data in MySql
        if mydb.is_connected():
            mycursor = mydb.cursor()

    def on_error(self, status_code):
        """
        Since Twitter API has rate limit, stop scrapping data as it exceed to the threshold
        """
        if status_code == 420:
            return False


stream = MyStreamListener(config.API_KEY,
                          config.SECRET_KEY,
                          config.ACCESS_TOKEN,
                          config.ACCESS_TOKEN_SECRET)

if __name__ == "__main__":
    mydb = mysql.connector.connect(
        host="localhost",
        user="adi",
        passwd="root",
        database="TwitterDB",
        #charset="utf-8"
    )

    if mydb.is_connected():
        """
        Check if the table exist if not then create a new one
        """
        mycursor=mydb.cursor()
        mycursor.execute(f"""
        SELECT SUM(TABLE_ROWS) 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = '{settings.TABLE_NAME}';
        """)
        print(mycursor)

        if mycursor.fetchone()[0] != 1:
            print("TABLE NOT EXIST")
        mycursor.close()
    #stream.filter(languages=["en"], track=settings.TRACK_WORDS)
