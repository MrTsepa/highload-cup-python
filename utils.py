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
        'as': 'location_full_arr'
    }},
    {'$addFields': {
        'location__distance': {'$arrayElemAt': ['$location_full_arr.distance', 0]},
        'location__country': {'$arrayElemAt': ['$location_full_arr.country', 0]},
        'place': {'$arrayElemAt': ['$location_full_arr.place', 0]},
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
]


def to_age(timestamp):
    d = datetime.datetime(1970, 1, 1) + datetime.timedelta(seconds=timestamp)
    return relativedelta(NOW_datetime, d).years


def delete_all(db):
    db.users.delete_many({})
    db.locations.delete_many({})
    db.visits.delete_many({})


def fill_db_train(db):
    delete_all(db)

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
    db.visits.aggregate(aggregation_query)
    print("Finished")
    print("Removing fields...")
    db.visits.update(
        {},
        {'$unset': {
            'location_full_arr': '',
            'user_full_arr': ''
        }},
        upsert=False, multi=True
    )
    print("Finished")
    print("Creating indexes...")
    db.visits.create_index("user")
    db.visits.create_index("location")
    print("Finished")
    one = db.visits.find_one()
    # print(one)


def fill_db_full(db):
    delete_all(db)
    print("Reading users...")
    for i in range(1, 100):
        path = '{}/users_{}.json'.format(PATH_TO_FULL, i)
        try:
            data = json.load(open(path, 'r'))
        except:
            break
        for user in data['users']:
            user['_id'] = user['id']
            user['age'] = to_age(user['birth_date'])
        db.users.insert_many(data['users'])
    print("Users count: " + str(db.users.find({}).count()))

    print("Reading locations...")
    for i in range(1, 100):
        path = '{}/locations_{}.json'.format(PATH_TO_FULL, i)
        try:
            data = json.load(open(path, 'r'))
        except:
            break
        for location in data['locations']:
            location['_id'] = location['id']
        db.locations.insert_many(data['locations'])
    print("Locations count: " + str(db.locations.find({}).count()))

    print("Reading visits...")
    for i in range(1, 100):
        path = '{}/visits_{}.json'.format(PATH_TO_FULL, i)
        try:
            data = json.load(open(path, 'r'))
        except:
            break
        for visit in data['visits']:
            visit['_id'] = visit['id']
        db.visits.insert_many(data['visits'])
    print("Visits count: " + str(db.visits.find({}).count()))
    t = time()
    print("Aggregating...")
    db.visits.aggregate(aggregation_query)
    print("Finished")
    print(time() - t)
    t = time()
    print("Creating indexes...")
    db.visits.create_index("user")
    db.visits.create_index("location")
    db.visits.create_index("visited_at")
    print("Finished")
    print(time() - t)
    # print(db.visits.find_one())
