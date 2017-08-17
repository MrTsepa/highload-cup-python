from bson.json_util import dumps
import bottle
import pymongo

from utils import fill_db2
from bottle.ext.mongo import MongoPlugin

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
    visits = mongodb.visits.find_one({'_id': id}, {'_id': False})
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
    result = mongodb.visits.find(
        filter_query,
        {'_id': False, 'id': False, 'user': False, 'location__country': False, 'location__distance': False}
    ).sort('visited_at', pymongo.ASCENDING)
    return dumps({'visits': result})


if __name__ == '__main__':
    fill_db2()
    app.run(host='localhost', port=8080)
