import bottle

from utils import fill_db2
from bottle.ext.mongo import MongoPlugin

app = bottle.Bottle()
plugin = MongoPlugin(uri="mongodb://127.0.0.1", db="highload", json_mongo=True)
app.install(plugin)


@app.get('/users/<id:int>')
def users_get(id, mongodb):
    user = mongodb.users.find_one({'id': id})
    if user:
        return user
    return bottle.HTTPError(404)


if __name__ == '__main__':
    fill_db2()
    app.run(host='localhost', port=8080)
