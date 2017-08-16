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
    db.users.insert_many(users_data['users'])

    locations_data = json.load(open(PATH_TO_DATA + '/locations_1.json', 'rb'))
    db.locations.insert_many(locations_data['locations'])

    visits_data = json.load(open(PATH_TO_DATA + '/visits_1.json', 'rb'))
    for visit in visits_data['visits']:
        location = db.locations.find_one({'id': visit['location']})
        visit['country'] = location['country']
        visit['distance'] = location['distance']
        db.visits.insert_one(visit)


if __name__ == '__main__':
    fill_db2()
