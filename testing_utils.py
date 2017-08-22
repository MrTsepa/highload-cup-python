# coding=utf-8
import datetime
import json as json
from multiprocessing.pool import Pool
from pprint import pprint
from time import time
from dateutil.relativedelta import relativedelta
from multiprocessing import Process

from pymongo import MongoClient

PATH_TO_DATA = 'data'
PATH_TO_FULL = 'data'

try:
    with open(PATH_TO_DATA+'/options.txt') as file:
        NOW_timestamp = int(file.readline())
except FileNotFoundError:
    NOW_timestamp = datetime.datetime.now().timestamp()
NOW_datetime = datetime.datetime.fromtimestamp(NOW_timestamp).replace(hour=0, minute=0, second=0)

aggregation_query = [
    {'$lookup': {
        'from': 'locations',
        'localField': 'location',
        'foreignField': '_id',
        'as': 'location_full'
    }},
    {'$unwind': '$location_full'},
    {'$addFields': {
        'location__distance': '$location_full.distance',
        'location__country': '$location_full.country',
        'place': '$location_full.place',
    }},
    {'$lookup': {
        'from': 'users',
        'localField': 'user',
        'foreignField': '_id',
        'as': 'user_full'
    }},
    {'$unwind': '$user_full'},
    {'$addFields': {
        'user__age': '$user_full.age',
        'user__gender': '$user_full.gender',
    }},
    {'$out': 'visits'}
]
aggregation_query1 = [
    {'$lookup': {
        'from': 'locations',
        'localField': 'location',
        'foreignField': '_id',
        'as': 'location_full'
    }},
    {'$unwind': '$location_full'},
    {'$addFields': {
        'location__distance': '$location_full.distance',
        'location__country': '$location_full.country',
        'place': '$location_full.place',
    }},
    {'$out': 'v1'}
]
aggregation_query2 = [
    {'$lookup': {
        'from': 'users',
        'localField': 'user',
        'foreignField': '_id',
        'as': 'user_full'
    }},
    {'$unwind': '$user_full'},
    {'$addFields': {
        'user__age': '$user_full.age',
        'user__gender': '$user_full.gender',
    }},
    {'$out': 'v2'}
]


def to_age(timestamp):
    d = datetime.datetime(1970, 1, 1) + datetime.timedelta(seconds=timestamp)
    return relativedelta(NOW_datetime, d).years


def delete_all(db):
    db.users.drop()
    db.locations.drop()
    db.visits.drop()
    db.v1.drop()
    db.v2.drop()


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
    one = db.visits.find_one()
    # print(one)


def f(q):
    client = MongoClient()
    db = client.highload
    db.visits.aggregate(q)


def fill_db_full(db):
    delete_all(db)
    print("Reading users...")
    for i in range(1, 100):
        path = '{}/users_{}.json'.format(PATH_TO_FULL, i)
        try:
            data = json.load(open(path, 'r', encoding='utf8'))
        except FileNotFoundError:
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
            data = json.load(open(path, 'r', encoding='utf8'))
        except FileNotFoundError:
            break
        for location in data['locations']:
            location['_id'] = location['id']
        db.locations.insert_many(data['locations'])
    print("Locations count: " + str(db.locations.find({}).count()))
    print("Reading visits...")
    for i in range(1, 100):
        path = '{}/visits_{}.json'.format(PATH_TO_FULL, i)
        try:
            data = json.load(open(path, 'r', encoding='utf8'))
        except FileNotFoundError:
            break
        for visit in data['visits']:
            visit['_id'] = visit['id']
        db.visits.insert_many(data['visits'])
    print("Visits count: " + str(db.visits.find({}).count()))
    # print("Creating indexes...")
    # t = time()
    # db.visits.create_index("user")
    # db.visits.create_index("location")
    # print("Finished in " + str(time() - t))
    # print("Aggregating...")
    t = time()

    db.visits.aggregate(aggregation_query)
    # db.visits.aggregate(aggregation_query1)
    # db.visits.aggregate(aggregation_query2)
    # print("Finished in " + str(time() - t))
    # db.visits.aggregate([
    #     {'$lookup': {
    #         'from': 'v1',
    #         'localField': '_id',
    #         'foreignField': '_id',
    #         'as': 'v1'
    #     }},
    #     {'$unwind': '$v1'},
    #     {'$out': 'visits'}
    # ])
    # explain = db.command('aggregate', 'locations', pipeline=aggregation_query, explain=False)
    # explain = db.visits.aggregate(aggregation_query, {'explain': True})
    # pprint(explain)
    print("Finished in " + str(time() - t))
    print(db.visits.find_one({}))
