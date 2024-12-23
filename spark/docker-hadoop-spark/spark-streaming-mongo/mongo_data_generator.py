import pymongo

mongo_url = 'mongodb://localhost:27017/'

client = pymongo.MongoClient(mongo_url)
client.foo.bar.insert_many([
    {"x": 1.0, "y": -1.0}, {"x": 0.0, "y": 4.0}])
client.close()