from bson import ObjectId
from pymongo import MongoClient

client = MongoClient("<use connection string from mongodb>")
# Define a database name "redi". If a given database not found, a new one will be created when inserting data.
database = client.redi

# Define a collection name "course". If a given collection not found, a new one will be created when inserting data.
collection = database.course
#collection.drop()
documents = [
    {
        "student": "John Doe",
        "location": "Munich",
        "skills": ["python", "MongoDB"],
        "age": 25
    },
    {
        "student": "Jane Doe",
        "location": "Munich",
        "skills": ["Java", "MySQL"],
        "age": 26
    },
    {
        "student": "Mike",
        "location": "Munich",
        "skills": ["Go", "Oracle"],
        "age": 24
    }

]

# Insert multiple documents
collection.insert_many(documents)

# Find all documents
students = collection.find({})

for student in students:
    print(student)

# Find document by Object ID
doc1 = collection.find_one({"_id": ObjectId("6715ff97608c0c645078f44d")})
print(doc1)

# Update a property within the document to a new value
doc1["age"] = 30

# Update a document using update_one(<filter>, <update>, options)
# upsert => Optional. When true, either:
# 1. Creates a new document if no documents match the filter. For more details see upsert behavior.
# 2. Updates a single document that matches the filter.
collection.update_one({"_id": ObjectId("6715ff97608c0c645078f44d")}, {"$set": doc1}, upsert=False)

# Delete a document using Object ID
collection.delete_one({"_id": ObjectId("6715ff97608c0c645078f44d")})
