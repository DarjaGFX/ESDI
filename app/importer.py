import os
from datetime import datetime, timedelta

from dotenv import load_dotenv
from elasticsearch import Elasticsearch

from text_process import PreProcess
from worker import app

load_dotenv()
source_host = os.getenv("source_host", '127.0.0.1')
source_port = os.getenv("source_port", '9200')
source_username = os.getenv("source_username", '')
source_password = os.getenv("source_password", '')
source_index = os.getenv("source_index", '')
dest_host = os.getenv("dest_host", '127.0.0.1')
dest_port = os.getenv("dest_port", '9200')
dest_username = os.getenv("dest_username", '')
dest_password = os.getenv("dest_password", '')
dest_tweet_index = os.getenv("dest_tweet_index", '')
dest_user_index = os.getenv("dest_user_index", '')


# Source Elasticsearch configuration
# cert = 'http_ca.crt'
es = Elasticsearch(f"https://{source_host}:{source_port}", basic_auth=(source_username,
                   source_password), timeout=50, retry_on_timeout=True, verify_certs=False)

# Dest Elasticsearch configuration
# cert = 'http_ca.crt'
es_dest = Elasticsearch(
    f"http://{dest_host}:{dest_port}", timeout=50, retry_on_timeout=True, verify_certs=False)


def get_period_tweets(start, end):
    try:
        q = {
            "bool": {
                "must": [],
                "should": [],
                "must_not": [],
                "filter": [{
                    "range": {
                        "created_at_dt": {
                            "gte": start,
                            "lte": end,
                            "format": "strict_date_optional_time"
                        }
                    }
                }]
            }
        }
        result = es.search(index=source_index, query=q, size=10000)
        return result
    except Exception as error:
        return None


def save_tweet(tweet):
    try:
        # Index the document into Elasticsearch
        # print(document)
        try:
            # print("delete")
            d = es_dest.delete(index=dest_tweet_index, id=tweet['id_str'])
            # print("delete")
            # print(f"{d=}")
        except Exception as error:
            pass

        ind = es_dest.index(
            index=dest_tweet_index, id=tweet['id_str'], document=tweet)
        return True
    except Exception as error:
        print(f"save_tweet: {error}")


def save_user(user_info):
    try:
        # Index the document into Elasticsearch
        # print(document)
        try:
            # print("delete")
            d = es_dest.delete(index=dest_user_index,
                               id=user_info['user_id_str'])
            # print("delete")
            # print(f"{d=}")
        except Exception as error:
            pass

        ind = es_dest.index(index=dest_user_index,
                            id=user_info['user_id_str'], document=user_info)
        return True
    except Exception as error:
        print(f"save_user: {error}")


def save_tweets(tweets):
    tweets_count = tweets.body['hits']['total']['value']
    for i in range(tweets_count):
        try:
            txt = tweets.body['hits']['hits'][i]['_source']['legacy']['full_text']
            id = tweets.body['hits']['hits'][i]['_source']['legacy']['id_str']
            try:
                user_url = tweets.body['hits']['hits'][i]['_source']['core']['user_results']['result']['legacy']['url']
            except Exception as error:
                user_url = ""
                pass
            tweet = {
                "tweet_text": txt,
                "cleaned_text": PreProcess(txt).deEmojify().Rpunc().Rnf().GetNouns().RS().text.replace('_', ' '),
                "lang": tweets.body['hits']['hits'][i]['_source']['legacy']['lang'],
                "quote_count": tweets.body['hits']['hits'][i]['_source']['legacy']['quote_count'],
                "reply_count": tweets.body['hits']['hits'][i]['_source']['legacy']['reply_count'],
                "retweet_count": tweets.body['hits']['hits'][i]['_source']['legacy']['retweet_count'],
                "id_str": tweets.body['hits']['hits'][i]['_source']['legacy']['id_str'],
                "user_id_str": tweets.body['hits']['hits'][i]['_source']['legacy']['user_id_str'],
                "category": tweets.body['hits']['hits'][i]['_source']['category'],
                "hashtag_list": tweets.body['hits']['hits'][i]['_source']['hashtag_list'],
                "created_at_dt": tweets.body['hits']['hits'][i]['_source']['created_at_dt'],
                "user_screen_name": tweets.body['hits']['hits'][i]['_source']['core']['user_results']['result']['legacy']['screen_name']
            }
            user_info = {
                "user_id_str": tweets.body['hits']['hits'][i]['_source']['legacy']['user_id_str'],
                "user_screen_name": tweets.body['hits']['hits'][i]['_source']['core']['user_results']['result']['legacy']['screen_name'],
                "favourites_count": tweets.body['hits']['hits'][i]['_source']['core']['user_results']['result']['legacy']['favourites_count'],
                "followers_count": tweets.body['hits']['hits'][i]['_source']['core']['user_results']['result']['legacy']['followers_count'],
                "friends_count": tweets.body['hits']['hits'][i]['_source']['core']['user_results']['result']['legacy']['friends_count'],
                "listed_count": tweets.body['hits']['hits'][i]['_source']['core']['user_results']['result']['legacy']['listed_count'],
                "media_count": tweets.body['hits']['hits'][i]['_source']['core']['user_results']['result']['legacy']['media_count'],
                "normal_followers_count": tweets.body['hits']['hits'][i]['_source']['core']['user_results']['result']['legacy']['normal_followers_count'],
                "statuses_count": tweets.body['hits']['hits'][i]['_source']['core']['user_results']['result']['legacy']['statuses_count'],
                "created_at": tweets.body['hits']['hits'][i]['_source']['core']['user_results']['result']['legacy']['created_at'],
                "url": user_url,
                "profile_image_url_https": tweets.body['hits']['hits'][i]['_source']['core']['user_results']['result']['legacy']['profile_image_url_https']
            }
            save_tweet(tweet)
            save_user(user_info)
        except Exception as error:
            print(f"save_tweets: {error}")


@app.task(queue='importer')
def minimporter(start: str, end: str):
    try:
        start: datetime = datetime.fromisoformat(start)
        end: datetime = datetime.fromisoformat(end)
        tweets = get_period_tweets(start.strftime(
            "%Y-%m-%dT%H:%M:%S.%fZ"), end.strftime("%Y-%m-%dT%H:%M:%S.%fZ"))
        if tweets:
            tweets_count = tweets.body['hits']['total']['value']
            # اگر توئیتی بازیابی شده باشد، توئیت ها و کاربران در الستیک ذخیره میشوند
            if tweets_count > 0:
                save_tweets(tweets)
    except Exception as e:
        print(f"minimporter: {str(e)}")


@app.task(queue='importer')
def main(from_date: str):
    from_date: datetime = datetime.fromisoformat(from_date)
    to_date: datetime = from_date + timedelta(minutes=59)
    # start = "2024-01-05T11:00:00.000Z"
    time_delta_min = 1
    # start = datetime.strptime(start, "%Y-%m-%dT%H:%M:%S.%fZ")
    start = from_date
    end = start + timedelta(minutes=time_delta_min)

    while end <= to_date:
        try:
            minimporter.delay(start=start.isoformat(), end=end.isoformat())
            start = end  # + timedelta(minutes= 1)
            end = start + timedelta(minutes=time_delta_min)
        except Exception as error:
            print(str(error))
    return
