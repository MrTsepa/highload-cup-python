import json
from pymongo import MongoClient

PATH_TO_DATA = 'data/TRAIN/data'


def fill_db2():
    client = MongoClient()
    db = client.highload
    db.users.delete_many({})
    db.locations.delete_many({})
    db.visits.delete_many({})

    users_data = json.load(open(PATH_TO_DATA + '/users_1.json', 'rb'))
    locations_data = json.load(open(PATH_TO_DATA + '/locations_1.json', 'rb'))
    visits_data = json.load(open(PATH_TO_DATA + '/visits_1.json', 'rb'))

    for user in users_data['users']:
        user['_id'] = user['id']
    db.users.insert_many((users_data['users']))

    for location in locations_data['locations']:
        location['_id'] = location['id']
    db.locations.insert_many(locations_data['locations'])

    for visit in visits_data['visits']:
        visit['_id'] = visit['id']
        location = db.locations.find_one({'_id': visit['location']})
        visit['location__country'] = location['country']
        visit['location__distance'] = location['distance']
    db.visits.insert_many(visits_data['visits'])


if __name__ == '__main__':
    fill_db2()
