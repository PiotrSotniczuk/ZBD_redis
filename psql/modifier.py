#!/usr/bin/python
from psycopg2 import connect, extensions, DatabaseError

from pgnotify import await_pg_notifications
import select
import random
import string

def country_generator():
    return ''.join(random.choice(string.ascii_uppercase) for _ in range(6))

def city_generator():
    return ''.join(random.choice(string.ascii_lowercase) for _ in range(6))

conn = None

if __name__ == '__main__':
    
    try:
        conn = connect(host="localhost", dbname="adds", user="ps", password="ps")
        conn.set_isolation_level(extensions.ISOLATION_LEVEL_AUTOCOMMIT)
                
        curs = conn.cursor()
        curs.execute("listen basic;")

        while True:

            if select.select([conn],[],[],5) == ([],[],[]):
                print("break with time")
                print(conn.notifies)
                break
            
            conn.poll()
            while len(conn.notifies) > 0:
                notify = conn.notifies.pop(0)
                id = notify.payload.strip()
                
                curs.execute("update ads set country=%s, city=%s where id=%s and city is null", [country_generator(), city_generator(),id])
                curs.execute("notify better, %s;", [id])
                conn.commit()

            if id == "-1":
                print("break  with id")
                break

    except (Exception, DatabaseError) as error:
            print(error)
    finally:
        if conn is not None:
            conn.close()
    print("psql modifier finito")