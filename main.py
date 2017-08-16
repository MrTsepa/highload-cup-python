import json
from bson.json_util import dumps
import bottle

from utils import fill_db2
from bottle.ext.mongo import MongoPlugin

app = bottle.Bottle()
plugin = MongoPlugin(uri="mongodb://127.0.0.1", db="highload", json_mongo=True)
app.install(plugin)


@app.get('/users/<id:int>')
def get_user(id, mongodb):
    user = mongodb.users.find_one({'id': id})
    if user:
        return user
    return bottle.HTTPError(404)


@app.get('/users/<id:int>/visits')
def get_user_visits(id, mongodb):
    fromDate = bottle.request.query.fromDate
    toDate = bottle.request.query.toDate
    country = bottle.request.query.country
    toDistance = bottle.request.query.toDistance
    filter_query = {'user': id}
    if len(fromDate) > 0:
        filter_query['visited_at']['$gt'] = int(fromDate)
    if len(toDate) > 0:
        filter_query['visited_at']['$lt'] = int(toDate)
    if len(country) > 0:
        filter_query['country'] = country
    result = mongodb.visits.find({'user': id}, {'_id': False})
    return dumps({'visits': result})


@app.get('/locations/<id:int>')
def get_location(id, mongodb):
    location = mongodb.locations.find_one({'id': id})
    if location:
        return location
    return bottle.HTTPError(404)


@app.get('/visits/<id:int>')
def get_visit(id, mongodb):
    visits = mongodb.visits.find_one({'id': id})  # TODO exclude
    if visits:
        return visits
    return bottle.HTTPError(404)


if __name__ == '__main__':
    fill_db2()
    app.run(host='localhost', port=8080)
