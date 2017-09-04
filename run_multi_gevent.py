# coding=utf-8
import urllib
from multiprocessing import Process

import sys


import pymongo
from gevent import monkey, sleep;
from pymongo import MongoClient

from utils import Timeit

monkey.patch_all()

from server import app


def run_gevent_app(host, port):
    print(port)
    app.run(host=host, port=port, server='gevent', quiet=False)


def create_indexes():
    client = MongoClient()
    db = client.highload
    with Timeit("User index...", v=1):
        db.visits.create_index([('user', pymongo.ASCENDING)], background=True)
    with Timeit("Location index...", v=1):
        db.visits.create_index([('location', pymongo.ASCENDING)], background=True)

if __name__ == '__main__':
    for port in sys.argv[2:]:
        p = Process(target=run_gevent_app, args=(sys.argv[1], port))
        p.start()
    # Process(target=create_indexes).start()

