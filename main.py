import os
import time
from random import randint
from threading import Thread
from typing import Callable

from dotenv import load_dotenv
from psycopg2._psycopg import connection as Connection
from psycopg2._psycopg import cursor as Cursor
from psycopg2.pool import ThreadedConnectionPool, AbstractConnectionPool

N = 10_000
THREADS_NUM = 10
IS_RANDOM = True
ROWS_NUM = 10_000


def generate_row_num() -> int:
    if (not IS_RANDOM) or (randint(0, 1) == 0):
        return 1
    else:
        return randint(2, ROWS_NUM)


def create_pool() -> ThreadedConnectionPool:
    load_dotenv()

    USER = os.getenv('USER')
    PASSWORD = os.getenv('PASSWORD')
    DATABASE = os.getenv('DATABASE')
    HOST = os.getenv('HOST')
    PORT = os.getenv('PORT')

    pool = ThreadedConnectionPool(minconn=1,
                                  maxconn=THREADS_NUM,
                                  user=USER,
                                  password=PASSWORD,
                                  database=DATABASE,
                                  host=HOST,
                                  port=PORT)
    return pool


def set_default_values(conn: Connection) -> None:
    cursor = conn.cursor()
    cursor.execute('''UPDATE user_counter SET counter = 0, version = 0''')
    conn.commit()


def execute_threads(thread_function: Callable[[Connection, Cursor], None], pool: AbstractConnectionPool) -> None:
    start = time.time()

    connections = [pool.getconn() for _ in range(THREADS_NUM)]
    threads = [Thread(target=thread_function, args=(conn, conn.cursor())) for conn in connections]

    set_default_values(connections[0])

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    for conn in connections:
        pool.putconn(conn)

    end = time.time()
    print(f'Total time taken for {thread_function.__name__}: {round(end - start, 2)}')


def lost_update(conn: Connection, cursor: Cursor) -> None:
    for _ in range(N):
        row = generate_row_num()
        cursor.execute(f'''SELECT counter FROM user_counter WHERE row = {row}''')
        counter = cursor.fetchone()[0]
        counter = counter + 1
        cursor.execute(f'''UPDATE user_counter SET counter = {counter} WHERE row = {row}''')
        conn.commit()


def inplace_update(conn: Connection, cursor: Cursor) -> None:
    for _ in range(N):
        row = generate_row_num()
        cursor.execute(f'''UPDATE user_counter SET counter = counter + 1 WHERE row = {row}''')
        conn.commit()


def row_level_locking(conn: Connection, cursor: Cursor) -> None:
    for _ in range(N):
        row = generate_row_num()
        cursor.execute(f'''SELECT counter FROM user_counter WHERE row = {row} FOR UPDATE''')
        counter = cursor.fetchone()[0]
        counter = counter + 1
        cursor.execute(f'''UPDATE user_counter SET counter = {counter} WHERE row = {row}''')
        conn.commit()


def optimistic_concurrency_control(conn: Connection, cursor: Cursor) -> None:
    for _ in range(N):
        row = generate_row_num()
        while True:
            cursor.execute(f'''SELECT counter, version FROM user_counter WHERE row = {row}''')
            counter, version = cursor.fetchone()
            counter = counter + 1
            cursor.execute(
                f'''UPDATE user_counter SET counter = {counter}, version = {version + 1} 
                WHERE row = {row} AND version = {version}'''
            )
            conn.commit()

            count = cursor.rowcount
            if count > 0:
                break


def main():
    try:
        pool = create_pool()
        execute_threads(lost_update, pool)
        execute_threads(inplace_update, pool)
        execute_threads(row_level_locking, pool)
        execute_threads(optimistic_concurrency_control, pool)
    finally:
        pool.closeall()


if __name__ == '__main__':
    main()
