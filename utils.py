import datetime
import json
from time import time
from dateutil.relativedelta import relativedelta

from pymongo import MongoClient

PATH_TO_DATA = 'data/TRAIN/data'
PATH_TO_FULL = 'data/FULL/data'
NOW_datetime = datetime.datetime.fromtimestamp(1502881943).replace(hour=0, minute=0, second=0)


def to_age(timestamp):
    d = datetime.datetime(1970, 1, 1) + datetime.timedelta(seconds=timestamp)
    return relativedelta(NOW_datetime, d).years


def fill_db():
    client = MongoClient()
    db = client.highload
    db.users.delete_many({})
    db.locations.delete_many({})
    db.visits.delete_many({})

    users_data = json.load(open(PATH_TO_DATA + '/users_1.json', 'r'))
    locations_data = json.load(open(PATH_TO_DATA + '/locations_1.json', 'r'))
    visits_data = json.load(open(PATH_TO_DATA + '/visits_1.json', 'r'))

    for user in users_data['users']:
        user['_id'] = user['id']
        user['age'] = to_age(user['birth_date'])
    db.users.insert_many((users_data['users']))

    for location in locations_data['locations']:
        location['_id'] = location['id']
    db.locations.insert_many(locations_data['locations'])

    for visit in visits_data['visits']:
        visit['_id'] = visit['id']
    db.visits.insert_many(visits_data['visits'])
    print("Aggregating...")
    db.visits.aggregate([
        {'$lookup': {
            'from': 'locations',
            'localField': 'location',
            'foreignField': '_id',
            'as': 'location_full_arr'
        }},
        {'$addFields': {
            'location__distance': {'$arrayElemAt': ['$location_full_arr.distance', 0]},
            'location__country': {'$arrayElemAt': ['$location_full_arr.country', 0]}
        }},
        {'$lookup': {
            'from': 'users',
            'localField': 'user',
            'foreignField': '_id',
            'as': 'user_full_arr'
        }},
        {'$addFields': {
            'user__age': {'$arrayElemAt': ['$user_full_arr.age', 0]},
        }},
        {'$out': 'visits'}
    ])
    print("Finished")
    print("Creating indexes...")
    db.visits.create_index("user")
    db.visits.create_index("location")
    print("Finished")
    one = db.visits.find_one()
    print(one)


def fill_db_full():
    client = MongoClient()
    db = client.highload
    db.users.delete_many({})
    db.locations.delete_many({})
    db.visits.delete_many({})
    print("Reading users...")
    for i in range(1, 22):
        path = '{}/users_{}.json'.format(PATH_TO_FULL, i)
        print(i)
        data = json.load(open(path, 'r'))
        for user in data['users']:
            user['_id'] = user['id']
            user['age'] = to_age(user['birth_date'])
        db.users.insert_many(data['users'])
    print("Users count: " + str(db.users.find({}).count()))

    print("Reading locations...")
    for i in range(1, 17):
        path = '{}/locations_{}.json'.format(PATH_TO_FULL, i)
        print(i)
        data = json.load(open(path, 'r'))
        for location in data['locations']:
            location['_id'] = location['id']
        db.locations.insert_many(data['locations'])
    print("Locations count: " + str(db.locations.find({}).count()))

    print("Reading visits...")
    for i in range(1, 22):
        path = '{}/visits_{}.json'.format(PATH_TO_FULL, i)
        print(i)
        data = json.load(open(path, 'r'))
        for visit in data['visits']:
            visit['_id'] = visit['id']
        db.visits.insert_many(data['visits'])
    print("Visits count: " + str(db.visits.find({}).count()))
    print("Aggregating...")
    db.visits.aggregate([
        {'$lookup': {
            'from': 'locations',
            'localField': 'location',
            'foreignField': '_id',
            'as': 'location_full_arr'
        }},
        {'$addFields': {
            'location__distance': {'$arrayElemAt': ['$location_full_arr.distance', 0]},
            'location__country': {'$arrayElemAt': ['$location_full_arr.country', 0]}
        }},
        {'$lookup': {
            'from': 'users',
            'localField': 'user',
            'foreignField': '_id',
            'as': 'user_full_arr'
        }},
        {'$addFields': {
            'user__age': {'$arrayElemAt': ['$user_full_arr.age', 0]},
            'user__gender': {'$arrayElemAt': ['$user_full_arr.gender', 0]},
        }},
        {'$out': 'visits'}
    ])
    print("Finished")
    print("Creating indexes...")
    db.visits.create_index("user")
    db.visits.create_index("location")
    print("Finished")
    print(db.visits.find_one())


if __name__ == '__main__':
    t = time()
    fill_db()
    # fill_db_full()
    print(time() - t)
