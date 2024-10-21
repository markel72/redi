from pymongo import MongoClient

client = MongoClient("<use connection string from mongodb>")
try:
    client.admin.command('ping')
    print("Successfully connected to MongoDB!")
except Exception as e:
    print(e)
