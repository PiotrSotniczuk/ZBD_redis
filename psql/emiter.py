#!/usr/bin/python
from psycopg2 import connect, extensions, DatabaseError

from pgnotify import await_pg_notifications
import select
import random
import string
from statistics import mean
import time

conn = None
times = []

if __name__ == '__main__':
    
    try:
        conn = connect(host="localhost", dbname="adds", user="ps", password="ps")
        conn.set_isolation_level(extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        conn2 = connect(host="localhost", dbname="adds", user="ps", password="ps")
        conn2.set_isolation_level(extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        max = 0.0

        curs = conn.cursor()
        curs2 = conn2.cursor()
        curs.execute("listen basic;")
        curs2.execute("listen better;")
        while True:

            if select.select([conn],[],[],5) == ([],[],[]):
                print("emiter break with time")
                print(conn.notifies)
                break
            
            conn.poll()
            while len(conn.notifies) > 0:
                notify = conn.notifies.pop(0)
                id = notify.payload.strip()

                decision = random.uniform(0.0, 1.0)
                if decision > 0.1:
                    while True:
                        if select.select([conn2],[],[],5) == ([],[],[]):
                            print("break waiting for modifier")
                            break
                        id2 = ""
                        conn2.poll()
                        while len(conn2.notifies) > 0:
                            notify2 = conn2.notifies.pop(0)
                            id2 = notify2.payload.strip()
                            if id2 == id:
                                break

                        if id2 == id:
                            break
                
                stat = ""
                if decision < 0.1:
                    stat = "basic"
                if decision > 0.4:
                    stat = "none"
                if decision < 0.4 and decision > 0.1:
                    stat = "better"
                
                curs.execute("update ads set stat=%s where id=%s and stat is null returning time;", [stat, id])
                ret = curs.fetchone()
                if ret is not None:
                    start = float(ret[0])
                    end = time.time() * 1000.0
                    delta = end - start
                    times.append(delta)
                    if delta > max:
                        max = delta 

            if id == "-1":
                print("break emit with id")
                break

    except (Exception, DatabaseError) as error:
            print(error)
    finally:
        if conn is not None:
            conn.close()
    print("psql emiter finito")
    if len(times) > 0:
        print("max-->",max)
        print("mean-->", mean(times))