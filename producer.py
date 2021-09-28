import json
import time
from confluent_kafka import Producer
import socket

from google_play_scraper import Sort, reviews_all

conf = {'bootstrap.servers': "pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092",
        'client.id': socket.gethostname(),
        'security.protocol':'SASL_SSL', 
        'sasl.mechanism':'PLAIN', 
        'sasl.username':'FL5OOZQ4OQPATRJZ', 
        'sasl.password':'v2Wbjn3yGNDlGyPwIO+2CRoswrIQC3Ej7d16HXwsrbFt02+9gieNsAt+CwUeImUD'}

producer = Producer(conf)

result = reviews_all(
    'com.havells.havellsone',
    sleep_milliseconds=0, # defaults to 0
    lang='en', # defaults to 'en'
    country='us', # defaults to 'us'
    sort=Sort.MOST_RELEVANT, # defaults to Sort.MOST_RELEVANT
    filter_score_with=5 # defaults to None(means all score)
)

obj = {
  'reviewId': '',
  'userName': '',
  'userImage': '',
  'content': '',
  'score': 0,
  'thumbsUpCount': 0,
  'reviewCreatedVersion': '',
  'replyContent': '',
  'repliedAt': ''
}

def main(event=None, context=None):
    for i in range(len(result)):
        registered_user = result[i]
        print(registered_user)
        obj['reviewId'] = registered_user['reviewId']
        obj['userName'] = registered_user['userName']
        obj['userImage'] = registered_user['userImage']
        obj['content'] = registered_user['content']
        obj['score'] = registered_user['score']
        obj['thumbsUpCount'] = registered_user['thumbsUpCount']
        obj['reviewCreatedVersion'] = registered_user['reviewCreatedVersion']
        obj['replyContent'] = registered_user['replyContent']
        obj['repliedAt'] = registered_user['repliedAt']
        producer.produce("test", key="key", value=json.dumps(obj))
        time.sleep(4)

# if __name__ == "__main__":
#     main()
