import redis
import random
import socket
import struct
import json
import time

from conf import redis_host, redis_port, redis_password

nr_people = 15000
data = []
for i in range(nr_people):
    data.append({"id": i, "IP": socket.inet_ntoa(struct.pack('>I', random.randint(1, 0xffffffff)))})

# random.shuffle(data)


if __name__ == '__main__':
    time.sleep(2.0)
    try:
        r = redis.StrictRedis(host=redis_host, port=redis_port, password=redis_password, decode_responses=True)
        r.flushdb()

        for row in data:
            r.set(row["id"], json.dumps(row))
            r.publish("basic", row["id"])

        r.publish("basic", -1)
       
    except Exception as e:
        print(e)
    
    print("adder finito")