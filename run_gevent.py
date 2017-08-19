# coding=utf-8
import sys
from gevent import monkey; monkey.patch_all()

from server import app

if __name__ == '__main__':
    app.run(host=sys.argv[1], port=int(sys.argv[2]), server='gevent')
