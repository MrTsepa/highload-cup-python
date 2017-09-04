# coding=utf-8

import sys
from pymongo import MongoClient

from utils import fill_db_full, Timeit, fill_db_inmem, fill_db_inmem_sqlite, fill_db_inmem_zip


def fill_db():
    client = MongoClient()
    db = client.highload
    with Timeit("Reading database...", v=1):
        fill_db_inmem_zip(db, sys.argv[1])

if __name__ == '__main__':
    fill_db()
