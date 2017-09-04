# coding=utf-8
import datetime
import json

import sqlite3
from zipfile import ZipFile

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

    def __init__(self, message='', v=2):
        self.message = message
        self.v = v
        self.start = None
        self.end = None

    def __enter__(self):
        if self.v == 2:
            print("+ " + self.message)
        self.start = time()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end = time()
        if self.v >= 1:
            print("- " + self.message + " in " + str(self.end - self.start))


def to_age(timestamp):
    d = datetime.datetime(1970, 1, 1) + datetime.timedelta(seconds=timestamp)
    return relativedelta(NOW_datetime, d).years


def delete_all(db):
    db.users.drop()
    db.locations.drop()
    db.visits.drop()


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


def fill_db_inmem(db, path_to_dir=PATH_TO_FULL):
    with Timeit("Clearing db"):
        delete_all(db)

    user_dict = {}
    with Timeit("Reading users..."):
        for i in range(1, 1000):
            path = '{}/users_{}.json'.format(path_to_dir, i)
            try:
                data = json.load(open(path, 'r'))
            except:
                break
            print(i)
            for user in data['users']:
                user['_id'] = user['id']
                user['age'] = to_age(user['birth_date'])
                user_dict[user['id']] = (user['age'], user['gender'])
            db.users.insert_many(data['users'])
    print(db.users.find({}).count())
    location_dict = {}
    with Timeit("Reading locations..."):
        for i in range(1, 1000):
            path = '{}/locations_{}.json'.format(path_to_dir, i)
            try:
                data = json.load(open(path, 'r'))
            except:
                break
            for location in data['locations']:
                location['_id'] = location['id']
                location_dict[location['id']] = (
                    location['distance'],
                    location['country'],
                    location['place']
                )
            db.locations.insert_many(data['locations'])
    # with Timeit("Creating indexes..."):
    #     db.visits.create_index("user")
    #     db.visits.create_index("location")

    with Timeit("Reading visits..."):
        for i in range(1, 1000):
            path = '{}/visits_{}.json'.format(path_to_dir, i)
            try:
                data = json.load(open(path, 'r'))
            except:
                break
            print(i)
            for visit in data['visits']:
                visit['_id'] = visit['id']

                visit['location__distance'], visit['location__country'], visit['place'] = \
                    location_dict[visit['location']]
                visit['user__age'], visit['user__gender'] = \
                    user_dict[visit['user']]
            db.visits.insert_many(data['visits'])
        #     visits.extend(data['visits'])
        # db.visits.insert_many(visits, ordered=False)

    print(db.users.find({}).count(), db.locations.find({}).count(), db.visits.find({}).count())



def fill_db_inmem_zip(db, path_to_zip):
    delete_all(db)
    archiv = ZipFile(path_to_zip)

    user_dict = {}
    with Timeit("Reading users...", v=1):
        for i in range(1, 1000):
            try:
                data = json.load(archiv.open('users_{}.json'.format(i)))
            except:
                break
            for user in data['users']:
                user['_id'] = user['id']
                user['age'] = to_age(user['birth_date'])
                user_dict[user['id']] = (user['age'], user['gender'])
            db.users.insert_many(data['users'])
    location_dict = {}
    with Timeit("Reading locations...", v=0):
        for i in range(1, 1000):
            try:
                data = json.load(archiv.open('locations_{}.json'.format(i)))
            except:
                break
            for location in data['locations']:
                location['_id'] = location['id']
                location_dict[location['id']] = (
                    location['distance'],
                    location['country'],
                    location['place']
                )

            db.locations.insert_many(data['locations'])

    # with Timeit("Indexes..."):
    #     db.visits.create_index("user")
    #     db.visits.create_index("location")

    with Timeit("Reading visits...", v=1):
        for i in range(1, 1000):
            try:
                data = json.load(archiv.open('visits_{}.json'.format(i)))
            except:
                break
            for visit in data['visits']:
                visit['_id'] = visit['id']

                visit['location__distance'], visit['location__country'], visit['place'] = \
                    location_dict[visit['location']]
                visit['user__age'], visit['user__gender'] = \
                    user_dict[visit['user']]
            db.visits.insert_many(data['visits'], ordered=False)

    print(db.users.find({}).count(), db.locations.find({}).count(), db.visits.find({}).count())


def fill_db_inmem_sqlite(db, path_to_dir=PATH_TO_FULL):
    with Timeit("Clearing db"):
        delete_all(db)

    conn = sqlite3.connect(':memory:')
    c = conn.cursor()
    c.execute('''CREATE TABLE users (
                  id int PRIMARY KEY, age int, gender text)''')
    c.execute('''CREATE TABLE locations (
                  id int PRIMARY KEY, distance int, country text, place text)''')
    with Timeit("Reading users..."):
        for i in range(1, 1000):
            path = '{}/users_{}.json'.format(path_to_dir, i)
            try:
                data = json.load(open(path, 'r'))
            except:
                break
            for user in data['users']:
                user['_id'] = user['id']
                user['age'] = to_age(user['birth_date'])
            c.executemany('''INSERT INTO users (id, age, gender) VALUES (?,?,?)''',
                          ((user['id'], user['age'], user['gender']) for user in data['users']))
            db.users.insert_many(data['users'])

    with Timeit("Reading locations..."):
        for i in range(1, 1000):
            path = '{}/locations_{}.json'.format(path_to_dir, i)
            try:
                data = json.load(open(path, 'r'))
            except:
                break
            for location in data['locations']:
                location['_id'] = location['id']
            c.execute('''INSERT INTO locations (id, distance, country, place) VALUES (?,?,?,?)''',
                      ((location['id'], location['distance'], location['country'], location['place']) for location in data['locations']))
            db.locations.insert_many(data['locations'])

    with Timeit("Reading visits..."):
        for i in range(1, 1000):
            path = '{}/visits_{}.json'.format(path_to_dir, i)
            try:
                data = json.load(open(path, 'r'))
            except:
                break
            for visit in data['visits']:
                visit['_id'] = visit['id']
                location__distance, location__country, place = c.execute(
                    "SELECT distance, country, place FROM locations WHERE id=?", (visit['location'],)
                ).fetchone()
                user__age, user__gender = c.execute(
                    "SELECT age, gender FROM users WHERE id=?", (visit['user'],)
                ).fetchone()
                visit['location__distance'] = location__distance
                visit['location__country'] = location__country
                visit['place'] = place
                visit['user__age'] = user__age
                visit['user__gender'] = user__gender
            db.visits.insert_many(data['visits'])

    print(db.users.find({}).count(), db.locations.find({}).count(), db.visits.find({}).count())

    with Timeit("Creating indexes..."):
        db.visits.create_index("user")
        db.visits.create_index("location")
        # db.visits.create_index("visited_at")
