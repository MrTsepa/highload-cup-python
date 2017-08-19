# coding=utf-8
import sys
from time import time

if __name__ == '__main__':
    print("Unzipping...")
    t = time()
    path_to_zip = sys.argv[1]
    import zipfile
    with zipfile.ZipFile(path_to_zip, "r") as zip_ref:
        zip_ref.extractall('data')
    print("Finished")
    print(time() - t)
