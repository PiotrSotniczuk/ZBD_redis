#!/usr/bin/python
from psycopg2 import connect, extensions, DatabaseError
import socket
import struct
import random
import time

nr_people = 1000
conn = None
data = []

if __name__ == '__main__':
    
    try:
        time.sleep(1.0)
        conn = connect(host="localhost", dbname="adds", user="ps", password="ps")
        conn.set_isolation_level(extensions.ISOLATION_LEVEL_AUTOCOMMIT)
               
        
        cur = conn.cursor()
        cur.execute("drop table ads;")
        cur.execute("create table ads(id integer primary key, ip text not null, country text, city text, time float, stat text);")
        conn.commit()

        for i in range(nr_people):
            row = {"id": i, "IP": socket.inet_ntoa(struct.pack('>I', random.randint(1, 0xffffffff)))}
            cur.execute("insert into ads values (%s,%s, null, null, %s);", (row["id"],row["IP"], time.time()*1000.0))
            cur.execute("notify basic, '%s';", [row["id"]])
            conn.commit()

        cur.execute("notify basic, '%s';", [-1])
        conn.commit()
        

    except (Exception, DatabaseError) as error:
            print(error)
    finally:
        if conn is not None:
            conn.close()
    print("psql adder finito")