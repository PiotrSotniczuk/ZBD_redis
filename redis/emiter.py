import redis
import json
import random
import time
import string
from datetime import datetime
from statistics import mean

from conf import redis_host, redis_port, redis_password
times = []

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
            if decision < 0.6:
                if decision >  0.1:
                    for mes in p_bet.listen():
                        if mes["data"] == id and mes["type"] == 'pmessage':
                            break

                if r.exists(id+'_END'):
                    continue

                if decision > 0.1:
                    r.set(id+'_END', 'better')
                else:
                    r.set(id + '_END', 'basic')
            else :
                if r.exists(id+'_END'):
                    continue
                r.set(id + '_END', 'none')

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
    