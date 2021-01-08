#!/usr/bin/python
from psycopg2 import connect, extensions, DatabaseError
import socket
import struct
import random
import time

nr_people = 150
conn = None
data = []
for i in range(nr_people):
    data.append({"id": i, "IP": socket.inet_ntoa(struct.pack('>I', random.randint(1, 0xffffffff)))})


if __name__ == '__main__':
    
    try:
        time.sleep(1.0)
        conn = connect(host="localhost", dbname="adds", user="ps", password="ps")
                
        
        cur = conn.cursor()
        cur.execute("drop table ads;")
        cur.execute("create table ads(id integer primary key, ip text not null, country text, city text);")
        conn.commit()

        for row in data:
            cur.execute("insert into ads values (%s,%s, null, null);", (row["id"],row["IP"]))
            cur.execute("notify basic, '%s';", [row["id"]])
            conn.commit()

        cur.execute("notify basic, '%s';", [-1])
        conn.commit()
        

    except (Exception, DatabaseError) as error:
            print(error)
    finally:
        if conn is not None:
            conn.close()
