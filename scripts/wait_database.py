import os
import time
import psycopg
end=time.monotonic()+60
while True:
    try:
        with psycopg.connect(os.environ["DATABASE_URL"],connect_timeout=3) as conn:
            conn.execute("SELECT 1")
        break
    except psycopg.OperationalError:
        if time.monotonic()>=end:
            raise
        time.sleep(2)
