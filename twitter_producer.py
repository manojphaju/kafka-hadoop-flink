# AAAAAAAAAAAAAAAAAAAAAAJOnwEAAAAAS1ZikhNk74k7Ydoi6HGnYTjA9TQ%3D62qVqMzRxvDcUg8ZBLGrz2KF7P7EMeNa28zGPWoPLhbHxjmgIy

import tweepy
import json
from json import dumps
from rich import print

from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['vps-data1:9092','vps-data2:9092','vps-data3:9092'],
                         value_serializer = lambda K: dumps(K).encode('utf-8'))


with open('secrets.json') as f:
    data = json.load(f)

consumer_key = data['CONSUMER_KEY']
consumer_secret = data['CONSUMER_SECRET']
access_token = data['ACCESS_TOKEN']
access_token_secret = data['ACCESS_TOKEN_SECRET']

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)
cursor = tweepy.Cursor(api.search_tweets, q='music', lang='en', tweet_mode='extended').items(50)

for tweet in cursor:
    hasttags = tweet.entities['hashtags']

    hashtext = list()

    for j in range(0, len(hasttags)):
        hashtext.append(hasttags[j]['text'])

    
    cur_data = {
        "id_str": tweet.id_str,
        "username": tweet.username,
        "tweet": tweet.full_text,
        "location": tweet.user.location,
        "created_at": tweet.created_at.isoformat(),
        "retweet_count": tweet.retwet_count,
        "favorite_count": tweet.favorite_count,
        "followers_count": tweet.user.followers_count,
        "lang": tweet.lang,
        "coordinates": tweet.coordinates
    }

    producer.send('my-test-topic', value=cur_data)
    print(cur_data)

