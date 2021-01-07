import redis
import json
import random
import string

from conf import redis_host, redis_port, redis_password

if __name__ == '__main__':
    
    try:
        r = redis.StrictRedis(host=redis_host, port=redis_port, password=redis_password, decode_responses=True)
        p_bas = r.pubsub()
        p_bas.psubscribe('basic')
        p_bet = r.pubsub()
        p_bet.psubscribe('better')


        for message in p_bas.listen():
            if message["type"] == 'psubscribe':
                continue

            decision = random.uniform(0.0, 1.0)
            id = message["data"]

            if id == '-1':
                break
            
            if decision < 0.6:
                if decision >  0.1:
                    for mes in p_bet.listen():
                        if mes["type"] == 'psubscribe':
                            continue    

                        if mes["data"] == id:
                            break

                row_js = r.get(id)
                if decision > 0.1:
                    r.rpush("better", row_js)
                else:
                    r.rpush("basic", row_js)
       
    except Exception as e:
        print(e)

    print("emiter finito")
    