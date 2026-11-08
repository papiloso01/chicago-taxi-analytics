"""Wait for the isolated CDC test database without inline CI heredocs."""
import os
import time

import psycopg
from psycopg.conninfo import conninfo_to_dict


def main():
    dsn = os.environ.get('CDC_TEST_DSN')
    if not dsn:
        raise SystemExit('Set CDC_TEST_DSN for the isolated cdc_test database')
    if conninfo_to_dict(dsn).get('dbname') != 'cdc_test':
        raise SystemExit('CDC tests require the disposable database named cdc_test')
    for attempt in range(30):
        try:
            with psycopg.connect(dsn, connect_timeout=3) as connection:
                connection.execute('SELECT 1')
            print('CDC test database is ready')
            return
        except psycopg.OperationalError:
            if attempt == 29:
                raise SystemExit('CDC test database unavailable after 30 attempts') from None
            time.sleep(1)


if __name__ == '__main__':
    main()
