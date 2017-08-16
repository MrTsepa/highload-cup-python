import sqlite3
import json
from pymongo import MongoClient

PATH_TO_DATA = 'data/TRAIN/data'


def fill_db2():
    client = MongoClient()
    db = client.highload
    db.users.delete_many({})

    data = json.load(open(PATH_TO_DATA + '/users_1.json', 'rb'))
    user = data['users'][0]
    db.users.insert_one(user)

def fill_db():
    db = sqlite3.connect("file:memdb1?mode=memory&cache=shared")
    db.execute('''CREATE TABLE IF NOT EXISTS user(
                    id INT,
                    email TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    gender TEXT,
                    birth_date INT);''')
    data = json.load(open(PATH_TO_DATA + '/users_1.json', 'rb'))
    user = data['users'][0]
    db.execute(" INSERT INTO user VALUES (?, ?, ?, ?, ?, ?)",
               (user['id'], user['email'], user['first_name'], user['last_name'], user['gender'], user['birth_date']))
    user = data['users'][1]
    db.execute(" INSERT INTO user VALUES (?, ?, ?, ?, ?, ?)",
               (user['id'], user['email'], user['first_name'], user['last_name'], user['gender'], user['birth_date']))
    print(db.execute("SELECT * FROM user").fetchall())


if __name__ == '__main__':
    fill_db2()
