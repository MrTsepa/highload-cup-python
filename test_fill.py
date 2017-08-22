from pymongo import MongoClient

from utils import fill_db_full, delete_all, Timeit

client = MongoClient()
db = client.highload
with Timeit("Reading database"):
    fill_db_full(db, '/home/stas/Workspace/highload-cup/hlcupdocs/data/TRAIN/data')
print(db.visits.find_one({}))
