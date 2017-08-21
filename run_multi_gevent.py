# coding=utf-8
from multiprocessing import Process

import sys
from gevent import monkey;

from fill_db_full import fill_db

monkey.patch_all()

from server import app


def run_gevent_app(host, port):
    print(port)
    app.run(host=host, port=port, server='gevent')


if __name__ == '__main__':
    for port in sys.argv[2:]:
        p = Process(target=run_gevent_app, args=(sys.argv[1], port))
        p.start()
