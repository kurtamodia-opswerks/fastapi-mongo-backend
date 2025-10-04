from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

uri = "mongodb://localhost:27017"
client = MongoClient(uri, server_api=ServerApi("1"))

db = client["viz_db"]
dataset_collection = db["datasets"]
