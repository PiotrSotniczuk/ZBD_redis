import redis
import json
import random
import time
import string
from datetime import datetime
from statistics import mean

from conf import redis_host, redis_port, redis_password
times = []

mod_lua="""if redis.call('exists', KEYS[1]) == 0 then if ARGV[1] < 0.1 then redis.call('set', KEYS[1], 'basic') end if ARGV[1] > 0.4 then redis.call('set', KEYS[1], 'none') end if ARGV[1] > 0.1 and ARGV[1] < 0.4 then redis.call('set', KEYS[1], 'better') end return redis.call('get', KEYS[2]) end return -1"""



if __name__ == '__main__':
    
    try:
        r = redis.StrictRedis(host=redis_host, port=redis_port, password=redis_password, decode_responses=True)
        p_bas = r.pubsub()
        p_bas.psubscribe('basic')
        p_bet = r.pubsub()
        p_bet.psubscribe('better')

        max = 0.0

        for message in p_bas.listen():
            if message["type"] == 'psubscribe':
                continue
            
            decision = random.uniform(0.0, 1.0)
            id = message["data"]
            # TODO add other workers and move to LUA
            if id == '-1':
                break
            
            if decision >  0.1:
                for mes in p_bet.listen():
                    if mes["data"] == id and mes["type"] == 'pmessage':
                        break

            if r.exists(id+'_END'):
                continue

            if decision < 0.1:
                 r.set(id + '_END', 'basic')

            if decision > 0.4:
                r.set(id + '_END', 'none')

            if decision > 0.1 and decision < 0.4:
                r.set(id + '_END', 'better')

            start = float(r.get(id +'_T'))
            end = time.time() * 1000.0
            delta = end - start
            times.append(delta)
            if delta > max:
                max = delta
    except Exception as e:
        print(e)

    print("emiter finito")
    print("max-->",max)
    print("mean-->", mean(times))
    