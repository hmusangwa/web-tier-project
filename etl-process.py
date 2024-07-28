import json
import psycopg2
from datetime import datetime

def load_data(tweets_file, dsn):
    conn = psycopg2.connect(dsn)
    cur = conn.cursor()

    with open(tweets_file, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                tweet = json.loads(line)
                
                # Handle user data
                user = tweet['user']
                user_query = """
                    INSERT INTO users (user_id, screen_name, description, followers_count, friends_count, listed_count, favourites_count, statuses_count, profile_image_url, profile_banner_url, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (user_id) DO NOTHING
                """
                user_data = (
                    user['id'], user['screen_name'], user['description'], user['followers_count'], user['friends_count'],
                    user['listed_count'], user['favourites_count'], user['statuses_count'], user['profile_image_url'],
                    user.get('profile_banner_url'), datetime.strptime(user['created_at'], '%a %b %d %H:%M:%S +0000 %Y')
                )
                cur.execute(user_query, user_data)
                
                # Handle tweet data
                tweet_query = """
                    INSERT INTO tweets (tweet_id, created_at, text, source, user_id, in_reply_to_user_id, in_reply_to_status_id, retweet_status, hashtags, lang)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                tweet_data = (
                    tweet['id'], datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y'), tweet['text'], tweet['source'],
                    tweet['user']['id'], tweet.get('in_reply_to_user_id'), tweet.get('in_reply_to_status_id'), 
                    json.dumps(tweet.get('retweeted_status')), [hashtag['text'] for hashtag in tweet['entities']['hashtags']],
                    tweet['lang']
                )
                cur.execute(tweet_query, tweet_data)

            except Exception as e:
                print(f"Error processing line: {line}\n{e}")

    conn.commit()
    cur.close()
    conn.close()

dsn = "dbname=koyebdb user=koyeb-adm password=UkQ2qMd9fEYP host=ep-orange-king-a2fy2snr.eu-central-1.pg.koyeb.app port=5432 sslmode=require options='-c endpoint=ep-orange-king-a2fy2snr'"
print (dsn)

load_data('query2_ref.txt', dsn)
