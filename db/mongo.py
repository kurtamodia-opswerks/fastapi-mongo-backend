from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
import os
from pymongo.errors import ConnectionFailure
import urllib.parse

# Load environment variables
load_dotenv()

username = os.getenv("MONGO_DB_USERNAME")
password = os.getenv("MONGO_DB_PASSWORD")
url = os.getenv("MONGO_DB_URL")

# Encode password in case it contains special characters like * or @
encoded_password = urllib.parse.quote_plus(password)

uri = f"mongodb+srv://{username}:{encoded_password}@{url}"

try:
    client = MongoClient(uri, server_api=ServerApi("1"))
    
    client.admin.command("ping")
    print("Successfully connected to MongoDB!")
    
    db = client["vizlydb"]
    print("📘 Database selected:", db.name)

except ConnectionFailure as e:
    print("Could not connect to MongoDB:", e)
except Exception as e:
    print("An error occurred:", e)


db = client["vizlydb"]
