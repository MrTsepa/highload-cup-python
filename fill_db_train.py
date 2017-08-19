# coding=utf-8
from time import time

from pymongo import MongoClient

from utils import fill_db_train

if __name__ == '__main__':
    client = MongoClient()
    db = client.highload_train
    t = time()
    fill_db_train(db)
    print(time() - t)
