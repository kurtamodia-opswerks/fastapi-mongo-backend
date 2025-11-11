from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

uri = "mongodb+srv://kurtmatthewamodia_db_user:Kmgwapo1*031300@vizly.srn8jql.mongodb.net/?appName=Vizly"
client = MongoClient(uri, server_api=ServerApi("1"))

db = client["vizlydb"]
