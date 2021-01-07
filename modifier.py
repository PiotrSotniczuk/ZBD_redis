import redis
import json
import random
import string

from conf import redis_host, redis_port, redis_password

def country_generator():
    return ''.join(random.choice(string.ascii_uppercase) for _ in range(6))

def city_generator():
    return ''.join(random.choice(string.ascii_lowercase) for _ in range(6))

if __name__ == '__main__':
    
    try:
        r = redis.StrictRedis(host=redis_host, port=redis_port, password=redis_password, decode_responses=True)
        p = r.pubsub()
        p.psubscribe('basic')
        for message in p.listen():
            print(message)
            row_js = r.get(message["data"])
            print(row_js)
            row = json.loads(row_js)

            row["Country"] = country_generator()
            row["City"] = city_generator()
            r.set(row["id"], json.dumps(row))

            r.publish("better", row["id"])
       
    except Exception as e:
        print(e)
    