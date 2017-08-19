# coding=utf-8
from time import time

from pymongo import MongoClient

from utils import fill_db_full

if __name__ == '__main__':
    client = MongoClient()
    db = client.highload
    t = time()
    fill_db_full(db)
    print(time() - t)
