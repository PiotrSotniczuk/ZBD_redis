import redis
import random
import socket
import struct
import json
import time
from datetime import datetime

from conf import redis_host, redis_port, redis_password

nr_people = 1000

if __name__ == '__main__':
    time.sleep(1.0)
    try:
        r = redis.StrictRedis(host=redis_host, port=redis_port, password=redis_password, decode_responses=True)
        r.flushdb()

        for i in range(nr_people):
            row = {"id": i, "IP": socket.inet_ntoa(struct.pack('>I', random.randint(1, 0xffffffff)))}
            r.set(str(i)+"_IP", row["IP"])
            r.set(str(i)+"_T", time.time()*1000.0)
            r.publish("basic", str(i))
            time.sleep(0.003)

        r.publish("basic", -1)
       
    except Exception as e:
        print(e)
    
    print("adder finito")