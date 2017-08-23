# coding=utf-8
from time import time

from pymongo import MongoClient

from utils import fill_db_full, Timeit, fill_db_inmem


def fill_db():
    client = MongoClient()
    db = client.highload
    with Timeit("Reading database..."):
        fill_db_inmem(db)

if __name__ == '__main__':
    fill_db()
