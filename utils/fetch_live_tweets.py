import tweepy
import logging
import config
import settings
import mysql.connector
import re

logging.basicConfig(filename="data.log", 
                    format='%(asctime)s %(message)s',
                    filemode='w')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

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
        if status.retweeted:
            # Avoid retweeted info and only original tweets will be reveived
            return True

        # Extract hashtags
        if len(status.entities['hashtags']) > 0:
            for i in range(0, len(status.entities['hashtags'])):
                hashtags.append(status.entities['hashtags'][i]['text'])

        # Extract Attributes from each tweet
        id_str = status.id_str
        created_at = status.created_at
        text = clean_tweet(status.text)
        user_created_at = status.user.created_at
        # is_user_verified = str(status.user.verified)
        user_location = de_emojify(status.user.location)
        user_description = de_emojify(status.user.description)
        user_followers_count = status.user.followers_count
        latitude = "None"
        longitude = "None"

        if status.coordinates is not None:
            longitude = status.coordinates['coordinates'][0]
            latitude = status.coordinates['coordinates'][1]

        retweet_count = status.retweet_count
        favorite_count = status.favourites_count

        print("Long: {}, Lati: {}".format(longitude, latitude))
        val = (id_str, created_at, text, user_created_at, user_location, user_description, user_followers_count, longitude, latitude, retweet_count, favorite_count)
        logger.info(val)
        # store all data in MySql
        if mydb.is_connected():
            mycursor = mydb.cursor()
            sql = "INSERT INTO {} (id_str, created_at, text, user_created_at, user_location, user_description, user_followers_count, longitude, latitude, retweet_count, favorite_count) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)".format(settings.TABLE_NAME)
            val = (id_str, created_at, text, user_created_at, user_location, user_description, user_followers_count, longitude, latitude, retweet_count, favorite_count)
            logger.info('Storing data in SQL')
            mycursor.execute(sql, val)
            mydb.commit()
            mycursor.close()

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
    )

    if mydb.is_connected():
        """
        Check if the table exist if not then create a new one
        """
        crsr = mydb.cursor(buffered=True)
        crsr.execute("SHOW TABLES")
        table_present = False
        for x in crsr:
            if x[0] == settings.TABLE_NAME[0]:
                table_present = True
                logger.info("Table already present")
                print("TABLE ALREADY PRESENT")

        if table_present == False:
            logger.info('creating a table')
            print("CREATING A TABLE")
            crsr.execute("CREATE TABLE programming ({})".format(settings.TABLE_ATTRIBUTES))
            mydb.commit()
        crsr.close()

    stream.filter(languages=["en"], track=settings.TRACK_WORDS)
