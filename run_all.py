from __future__ import division

# coding=utf-8
import json
import urllib
from time import time

import gevent
import pymongo
import bottle

from multiprocessing import Process

import sys
from gevent import monkey, sleep
monkey.patch_all()

from pymongo import MongoClient
from bson.json_util import dumps
from bottle.ext.mongo import MongoPlugin

from utils import to_age, delete_all


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


class Storage:
    @classmethod
    def get_user(cls, id):
        raise NotImplementedError

    @classmethod
    def update_user(cls, id, data):
        raise NotImplementedError

    @classmethod
    def insert_user(cls, data):
        raise NotImplementedError

    @classmethod
    def get_location(cls, id):
        raise NotImplementedError

    @classmethod
    def update_location(cls, id, data):
        raise NotImplementedError

    @classmethod
    def insert_location(cls, data):
        raise NotImplementedError


class InmemStorage(Storage):
    user_dict = {}
    location_dict = {}

    @classmethod
    def get_user(cls, id):
        if id not in cls.user_dict:
            return None
        return cls.user_dict[id]

    @classmethod
    def update_user(cls, id, data):
        user = cls.user_dict[id]
        for key in data:
            user[key] = data[key]

    @classmethod
    def insert_user(cls, data):
        cls.user_dict[data['id']] = data

    @classmethod
    def get_location(cls, id):
        if id not in cls.location_dict:
            return None
        location = cls.location_dict[id]
        return {key: location[key] for key in location if key in ['id', 'place', 'country', 'city', 'distance']}

    @classmethod
    def update_location(cls, id, data):
        location = cls.location_dict[id]
        for key in data:
            location[key] = data[key]

    @classmethod
    def insert_location(cls, data):
        cls.location_dict[data['id']] = data


class MongoStorage(Storage):
    pass

app = bottle.Bottle()
plugin = MongoPlugin(uri="mongodb://127.0.0.1", db="highload", json_mongo=True)
app.install(plugin)


s = InmemStorage


@app.get('/users/<id:int>')
def get_user(id, mongodb):
    user = s.get_user(id)
    if user:
        return {key: user[key] for key in user if key not in ('_id', 'age')}
    return bottle.HTTPError(404)


@app.get('/locations/<id:int>')
def get_location(id, mongodb):
    location = s.get_location(id)
    if location:
        return location
    return bottle.HTTPError(404)


@app.get('/visits/<id:int>')
def get_visit(id, mongodb):
    visits = mongodb.visits.find_one({'_id': id}, {
            '_id': False, 'location__country': False, 'location__distance': False,
            'location_full': False, 'user_full': False, 'user__age': False,
            'user__gender': False, 'place': False
        })
    if visits:
        return visits
    return bottle.HTTPError(404)


@app.get('/users/<id:int>/visits')
def get_user_visits(id, mongodb):
    try:
        with gevent.Timeout(2):
            if not s.get_user(id):
                return bottle.HTTPError(404)
            fromDate = bottle.request.query.fromDate
            toDate = bottle.request.query.toDate
            country = bottle.request.query.country
            toDistance = bottle.request.query.toDistance
            filter_query = {'user': id}
            try:
                if fromDate:
                    if 'visited_at' not in filter_query:
                        filter_query['visited_at'] = {}
                    filter_query['visited_at']['$gt'] = int(fromDate)
                if toDate:
                    if 'visited_at' not in filter_query:
                        filter_query['visited_at'] = {}
                    filter_query['visited_at']['$lt'] = int(toDate)
                if country:
                    filter_query['location__country'] = country
                if toDistance:
                    if 'location__distance' not in filter_query:
                        filter_query['location__distance'] = {}
                    filter_query['location__distance']['$lt'] = int(toDistance)
            except ValueError:
                return bottle.HTTPError(400)
            result = mongodb.visits.find(
                filter_query, {
                    '_id': False, 'id': False, 'user': False,
                    'location__country': False, 'location__distance': False,
                    'location_full': False, 'user_full': False,
                    'user__age': False, 'user__gender': False,
                    'location': False
                }
            ).sort('visited_at', pymongo.ASCENDING)
            return dumps({'visits': result})
    except:
        return {'visits': []}


@app.get('/locations/<id:int>/avg')
def get_location_avg(id, mongodb):
    try:
        with gevent.Timeout(2):
            if not s.get_location(id):
                return bottle.HTTPError(404)
            fromDate = bottle.request.query.fromDate
            toDate = bottle.request.query.toDate
            fromAge = bottle.request.query.fromAge
            toAge = bottle.request.query.toAge
            gender = bottle.request.query.gender

            filter_query = {'location': id}
            try:
                if fromDate:
                    if 'visited_at' not in filter_query:
                        filter_query['visited_at'] = {}
                    filter_query['visited_at']['$gt'] = int(fromDate)
                if toDate:
                    if 'visited_at' not in filter_query:
                        filter_query['visited_at'] = {}
                    filter_query['visited_at']['$lt'] = int(toDate)
                if fromAge:
                    if 'user__age' not in filter_query:
                        filter_query['user__age'] = {}
                    filter_query['user__age']['$gte'] = int(fromAge)
                if toAge:
                    if 'user__age' not in filter_query:
                        filter_query['user__age'] = {}
                    filter_query['user__age']['$lt'] = int(toAge)
                if gender:
                    if gender not in ('m', 'f'):
                        return bottle.HTTPError(400)
                    filter_query['user__gender'] = gender
            except ValueError:
                return bottle.HTTPError(400)

            result = mongodb.visits.find(filter_query, {'mark': True})
            c = result.count()
            if c == 0:
                avg = 0
            else:
                avg = sum(v['mark'] for v in result) / c
            return {'avg': round(avg, 5)}
    except:
        return {'avg': 0}


@app.get('/hello')
def hello():
    with gevent.Timeout(2):
        gevent.sleep(1)
        return 'Hello'


@app.post('/users/<id:int>')
def update_user(id, mongodb):
    user = s.get_user(id)
    if not user:
        return bottle.HTTPError(404)
    data = bottle.request.json
    if not data:
        return bottle.HTTPError(400)
    if 'id' in data:
        return bottle.HTTPError(400)
    for key in data:
        if data[key] is None:
            return bottle.HTTPError(400)
    # TODO validate
    user_update = data
    new_age = None
    if 'birth_date' in data:
        new_age = to_age(data['birth_date'])
        if new_age != user['age']:
            user_update['age'] = new_age
    # mongodb.users.update(
    #     {"_id": id},
    #     {"$set": user_update}
    # )
    s.update_user(id, user_update)
    visits_update = {}
    if 'gender' in data:
        visits_update['user__gender'] = data['gender']
    if new_age is not None:
        visits_update['user__age'] = new_age
    if visits_update:
        mongodb.visits.update(
            {"user": id},
            {"$set": visits_update},
            upsert=False, multi=True
        )
    return {}


@app.post('/locations/<id:int>')
def update_location(id, mongodb):
    location = s.get_location(id)
    if not location:
        return bottle.HTTPError(404)
    data = bottle.request.json
    if not data:
        return bottle.HTTPError(400)
    if 'id' in data:
        return bottle.HTTPError(400)
    for key in data:
        if data[key] is None:
            return bottle.HTTPError(400)
    # TODO validate
    # mongodb.locations.update(
    #     {"_id": id},
    #     {"$set": data}
    # )
    s.update_location(id, data)
    visits_update = {}
    if 'country' in data:
        visits_update['location__country'] = data['country']
    if 'distance' in data:
        visits_update['location__distance'] = data['distance']
    if 'place' in data:
        visits_update['place'] = data['place']
    if visits_update:
        mongodb.visits.update(
            {"location": id},
            {"$set": visits_update},
            upsert=False, multi=True
        )
    return {}


@app.post('/visits/<id:int>')
def update_visit(id, mongodb):
    visit = mongodb.visits.find_one({'_id': id})
    if not visit:
        return bottle.HTTPError(404)
    data = bottle.request.json
    if not data:
        return bottle.HTTPError(400)
    if 'id' in data:
        return bottle.HTTPError(400)
    for key in data:
        if data[key] is None:
            return bottle.HTTPError(400)
    # TODO validate

    if 'user' in data:
        user = s.get_user(data['user'])
        if not user:
            return bottle.HTTPError(400)
        data['user__age'] = user['age']
        data['user__gender'] = user['gender']
    if 'location' in data:
        location = s.get_location(data['location'])
        if not location:
            return bottle.HTTPError(400)
        data['location__distance'] = location['distance']
        data['location__country'] = location['country']
        data['place'] = location['place']
    mongodb.visits.update(
        {"_id": id},
        {"$set": data}
    )
    return {}


@app.post('/users/new')
def new_user(mongodb):
    data = bottle.request.json
    if not data:
        return bottle.HTTPError(400)
    for key in data:
        if data[key] is None:
            return bottle.HTTPError(400)
    if 'id' in data:
        if s.get_user(data['id']):
            return bottle.HTTPError(400)
    # TODO validate
    data['_id'] = data['id']
    data['age'] = to_age(data['birth_date'])
    s.insert_user(data)
    return {}


@app.post('/locations/new')
def new_location(mongodb):
    data = bottle.request.json
    if not data:
        return bottle.HTTPError(400)
    for key in data:
        if data[key] is None:
            return bottle.HTTPError(400)
    if 'id' in data:
        # if mongodb.locations.find_one({'_id': data['id']}):
        if s.get_location(data['id']):
            return bottle.HTTPError(400)
    # TODO validate
    data['_id'] = data['id']
    # mongodb.locations.insert(data)
    s.insert_location(data)
    return {}


@app.post('/visits/new')
def new_visit(mongodb):
    data = bottle.request.json
    if not data:
        return bottle.HTTPError(400)
    for key in data:
        if data[key] is None:
            return bottle.HTTPError(400)
    if 'id' in data:
        if mongodb.visits.find_one({'_id': data['id']}):
            return bottle.HTTPError(400)
    # TODO validate
    # user = mongodb.users.find_one({'_id': data['user']})
    user = s.get_user(data['user'])
    if not user:
        return bottle.HTTPError(400)
    # location = mongodb.locations.find_one({'_id': data['location']})
    location = s.get_location(data['location'])
    if not location:
        return bottle.HTTPError(400)

    data['location__distance'] = location['distance']
    data['location__country'] = location['country']
    data['place'] = location['place']
    data['user__age'] = user['age']
    data['user__gender'] = user['gender']

    data['_id'] = data['id']
    mongodb.visits.insert(data)
    return {}


def fill_db_inmem(db, path_to_dir):
    with Timeit("Clearing db"):
        delete_all(db)
    users = []
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
                s.user_dict[user['id']] = user
            users.extend(data['users'])

    locations = []
    with Timeit("Reading locations..."):
        for i in range(1, 100):
            path = '{}/locations_{}.json'.format(path_to_dir, i)
            try:
                data = json.load(open(path, 'r'))
            except:
                break
            for location in data['locations']:
                location['_id'] = location['id']
                s.location_dict[location['id']] = location
            locations.extend(data['locations'])

    visits = []
    with Timeit("Reading visits..."):
        for i in range(1, 100):
            path = '{}/visits_{}.json'.format(path_to_dir, i)
            try:
                data = json.load(open(path, 'r'))
            except:
                break
            for visit in data['visits']:
                visit['_id'] = visit['id']
                location = s.location_dict[visit['location']]
                user = s.user_dict[visit['user']]
                visit['location__distance'] = location['distance']
                visit['location__country'] = location['country']
                visit['place'] = location['place']
                visit['user__age'] = user['age']
                visit['user__gender'] = user['gender']
            visits.extend(data['visits'])

    with Timeit("Inserting..."):
        db.visits.insert_many(visits)
        print("Users count: " + str(len(s.user_dict)))
        print("Locations count: " + str(len(s.location_dict)))
        print("Visits count: " + str(db.visits.find({}).count()))

    with Timeit("Creating indexes..."):
        db.visits.create_index("user")
        db.visits.create_index("location")
        db.visits.create_index("location__distance")
        db.visits.create_index("location__country")
        db.visits.create_index("place")
        db.visits.create_index("user__age")
        db.visits.create_index("user__gender")


def run_gevent_app(host, port):
    print(port)
    app.run(host=host, port=port, server='gevent', quiet=False)


def heat(ports):
    sleep(10)
    for _ in range(2):
        for port in ports:
            urllib.urlopen('http://127.0.0.1:' + port + '/users/1')
        for port in ports:
            urllib.urlopen('http://127.0.0.1:' + port + '/users/100/visits')
        for port in ports:
            urllib.urlopen('http://127.0.0.1:' + port + '/locations/500/avg')

if __name__ == '__main__':
    client = MongoClient()
    db = client.highload
    with Timeit("Reading database..."):
        fill_db_inmem(db, path_to_dir=sys.argv[1])

    Process(target=heat, args=(sys.argv[3:],)).start()
    if len(sys.argv[3:]) > 1:
        for port in sys.argv[3:]:
            p = Process(target=run_gevent_app, args=(sys.argv[2], port))
            p.start()
    else:
        run_gevent_app(sys.argv[2], sys.argv[3])
