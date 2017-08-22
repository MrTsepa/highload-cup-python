from __future__ import unicode_literals, division

# coding=utf-8
import gevent
from gevent import monkey; monkey.patch_all()
from bson.json_util import dumps
import bottle
import pymongo
from bottle.ext.mongo import MongoPlugin

from utils import to_age

app = bottle.Bottle()
plugin = MongoPlugin(uri="mongodb://127.0.0.1", db="highload", json_mongo=True)
app.install(plugin)


@app.get('/users/<id:int>')
def get_user(id, mongodb):
    user = mongodb.users.find_one({'_id': id}, {'_id': False, 'age': False})
    if user:
        return user
    return bottle.HTTPError(404)


@app.get('/locations/<id:int>')
def get_location(id, mongodb):
    location = mongodb.locations.find_one({'_id': id}, {'_id': False})
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
            if not mongodb.users.find_one({'_id': id}):
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
            if not mongodb.locations.find_one({'_id': id}):
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
    user = mongodb.users.find_one({'_id': id})
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
    mongodb.users.update(
        {"_id": id},
        {"$set": user_update}
    )
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
    location = mongodb.locations.find_one({'_id': id})
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
    mongodb.locations.update(
        {"_id": id},
        {"$set": data}
    )
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
        user = mongodb.users.find_one({'_id': data['user']})
        if not user:
            return bottle.HTTPError(400)
        data['user__age'] = user['age']
        data['user__gender'] = user['gender']
    if 'location' in data:
        location = mongodb.locations.find_one({'_id': data['location']})
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
        if mongodb.users.find_one({'_id': data['id']}):
            return bottle.HTTPError(400)
    # TODO validate
    data['_id'] = data['id']
    data['age'] = to_age(data['birth_date'])
    mongodb.users.insert(data)
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
        if mongodb.locations.find_one({'_id': data['id']}):
            return bottle.HTTPError(400)
    # TODO validate
    data['_id'] = data['id']
    mongodb.locations.insert(data)
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
    user = mongodb.users.find_one({'_id': data['user']})
    if not user:
        return bottle.HTTPError(400)
    location = mongodb.locations.find_one({'_id': data['location']})
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
