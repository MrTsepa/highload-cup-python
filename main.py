from bson.json_util import dumps
import bottle
import pymongo

from utils import fill_db2
from bottle.ext.mongo import MongoPlugin

from gevent import monkey; monkey.patch_all()

app = bottle.Bottle()
plugin = MongoPlugin(uri="mongodb://127.0.0.1", db="highload", json_mongo=True)
app.install(plugin)


@app.get('/users/<id:int>')
def get_user(id, mongodb):
    user = mongodb.users.find_one({'_id': id}, {'_id': False})
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
            'location_full_arr': False, 'user_full_arr': False, 'user__age': False
        })
    if visits:
        return visits
    return bottle.HTTPError(404)


@app.get('/users/<id:int>/visits')
def get_user_visits(id, mongodb):
    if mongodb.users.find({'_id': id}, {'_id': 1}).limit(1).count(with_limit_and_skip=True) == 0:
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
            '_id': False, 'id': False, 'user': False, 'location__country': False, 'location__distance': False,
            'location_full_arr': False, 'user_full_arr': False, 'user__age': False
        }
    ).sort('visited_at', pymongo.ASCENDING)
    return dumps({'visits': result})


@app.get('/locations/<id:int>/avg')
def get_location_avg(id, mongodb):
    if mongodb.locations.find({'_id': id}, {'_id': 1}).limit(1).count(with_limit_and_skip=True) == 0:
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
            filter_query['user__age']['$gt'] = int(fromAge)
        if toAge:
            if 'user__age' not in filter_query:
                filter_query['user__age'] = {}
            filter_query['user__age']['$lt'] = int(toAge)
        if gender:
            filter_query['user__gender'] = gender
    except ValueError:
        return bottle.HTTPError(400)

    result = mongodb.visits.find(filter_query, {'mark': True})
    if result.count() == 0:
        avg = 0
    else:
        avg = sum(v['mark'] for v in result) / result.count()
    return dumps({'avg': round(avg, 5)})

if __name__ == '__main__':
    # fill_db2()
    app.run(host='localhost', port=8080, server='gevent')
