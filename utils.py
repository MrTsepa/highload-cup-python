from __future__ import unicode_literals

# coding=utf-8
import datetime
import json
from dateutil.relativedelta import relativedelta
from time import time
PATH_TO_DATA = 'data'
PATH_TO_FULL = 'data'

try:
    with open(PATH_TO_DATA+'/options.txt') as file:
        NOW_timestamp = int(file.readline())
except Exception:
    NOW_timestamp = (datetime.datetime.now() - datetime.datetime(1970, 1, 1)).total_seconds()
NOW_datetime = datetime.datetime.fromtimestamp(NOW_timestamp).replace(hour=0, minute=0, second=0)

aggregation_query = [
    {'$lookup': {
        'from': 'locations',
        'localField': 'location',
        'foreignField': '_id',
        'as': 'location_full'
    }},
    {'$unwind': '$location_full'},
    {'$lookup': {
        'from': 'users',
        'localField': 'user',
        'foreignField': '_id',
        'as': 'user_full'
    }},
    {'$unwind': '$user_full'},
    {'$addFields': {
        'location__distance': '$location_full.distance',
        'location__country': '$location_full.country',
        'place': '$location_full.place',
        'user__age': '$user_full.age',
        'user__gender': '$user_full.gender',
    }},
    {'$out': 'visits'}
]


class Timeit:

    def __init__(self, message=''):
        self.message = message
        self.start = None
        self.end = None

    def __enter__(self):
        print("+ " + self.message)
        self.start = time()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end = time()
        print("- " + self.message + " in " + str(self.end - self.start))


def to_age(timestamp):
    d = datetime.datetime(1970, 1, 1) + datetime.timedelta(seconds=timestamp)
    return relativedelta(NOW_datetime, d).years


def delete_all(db):
    db.users.delete_many({})
    db.locations.delete_many({})
    db.visits.delete_many({})


def fill_db_full(db, path_to_dir=PATH_TO_FULL):
    with Timeit("Clearing db"):
        delete_all(db)
    with Timeit("Reading users..."):
        for i in range(1, 100):
            path = '{}/users_{}.json'.format(path_to_dir, i)
            try:
                data = json.load(open(path, 'r'))
            except:
                break
            for user in data['users']:
                user['_id'] = user['id']
                user['age'] = to_age(user['birth_date'])
            db.users.insert_many(data['users'])
        print("Users count: " + str(db.users.find({}).count()))

    with Timeit("Reading locations..."):
        for i in range(1, 100):
            path = '{}/locations_{}.json'.format(path_to_dir, i)
            try:
                data = json.load(open(path, 'r'))
            except:
                break
            for location in data['locations']:
                location['_id'] = location['id']
            db.locations.insert_many(data['locations'])
        print("Locations count: " + str(db.locations.find({}).count()))

    with Timeit("Reading visits..."):
        for i in range(1, 100):
            path = '{}/visits_{}.json'.format(path_to_dir, i)
            try:
                data = json.load(open(path, 'r'))
            except:
                break
            for visit in data['visits']:
                visit['_id'] = visit['id']
            db.visits.insert_many(data['visits'])
        print("Visits count: " + str(db.visits.find({}).count()))

    with Timeit("Aggregating..."):
        db.visits.aggregate(aggregation_query)

    with Timeit("Creating indexes..."):
        db.visits.create_index("user")
        db.visits.create_index("location")
        # db.visits.create_index("visited_at")
