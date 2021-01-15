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
            if message["type"] == 'psubscribe':
                continue

            id = message["data"]

            if id == '-1':
                break

            r.set(id+'_GEO', country_generator() + ' ' + city_generator())

            r.publish("better", id)
       
    except Exception as e:
        print(e)
    
    print("redis modifier finito")