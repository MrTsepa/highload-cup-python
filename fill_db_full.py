# coding=utf-8
from time import time

from pymongo import MongoClient

from utils import fill_db_full, Timeit


def fill_db():
    client = MongoClient()
    db = client.highload
    with Timeit("Reading database..."):
        fill_db_full(db)

if __name__ == '__main__':
    fill_db()
