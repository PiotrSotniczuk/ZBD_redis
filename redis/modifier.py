import redis
import json
import random
import string

from conf import redis_host, redis_port, redis_password

def country_generator():
    return ''.join(random.choice(string.ascii_uppercase) for _ in range(6))

def city_generator():
    return ''.join(random.choice(string.ascii_lowercase) for _ in range(6))

mod_lua="""if redis.call('exists', KEYS[1]) == 0 then redis.call('set', KEYS[1], ARGV[1]) redis.call('publish', 'better', ARGV[2]) end"""

if __name__ == '__main__':
    
    try:
        r = redis.StrictRedis(host=redis_host, port=redis_port, password=redis_password, decode_responses=True)
        p = r.pubsub()
        p.psubscribe('basic')
        modify = r.register_script(mod_lua)

        for message in p.listen():
            if message["type"] == 'psubscribe':
                continue

            id = message["data"]

            if id == '-1':
                break

            modify(keys=[id+'_GEO'], args=[country_generator()+ ' ' +city_generator(), id])
       
    except Exception as e:
        print(e)
    
    print("redis modifier finito")