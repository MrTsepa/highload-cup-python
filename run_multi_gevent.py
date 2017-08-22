# coding=utf-8
from multiprocessing import Process

import sys

from gevent import monkey, sleep; monkey.patch_all()

from server import app


def run_gevent_app(host, port):
    print(port)
    app.run(host=host, port=port, server='gevent')


if __name__ == '__main__':
    for port in sys.argv[2:]:
        p = Process(target=run_gevent_app, args=(sys.argv[1], port))
        p.start()
    # sleep(1)
    # for port in sys.argv[2:]:
    #     urllib.urlopen('http://127.0.0.1:' + str(port) + '/users/1')
