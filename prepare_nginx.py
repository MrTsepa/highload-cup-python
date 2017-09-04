import sys

import shutil

with open(sys.argv[1] + '/options.txt') as f:
    f.readline()
    is_train = not bool(int(f.readline()))
    print("Is train:", is_train)
    if is_train:
        shutil.copyfile('nginx_balance_train.conf', '/etc/nginx/sites-enabled/default')
        print("nginx.conf changed to train")
