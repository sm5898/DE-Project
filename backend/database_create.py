import pandas as pd
from pymongo import MongoClient

# 1️⃣ Connect to local MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client.cleanbite
collection = db.restaurants

# 2️⃣ Read your CSV file
csv_path = "backend/restaurants.csv"  # make sure it's in the same folder
print(f"Loading data from {csv_path} ...")
df = pd.read_csv(csv_path, encoding='latin1')  # or 'latin1'

# 3️⃣ Convert to dictionary format
data_dict = df.to_dict(orient="records")

# 4️⃣ Write to MongoDB
collection.delete_many({})  # optional: clear old data
collection.insert_many(data_dict)
print(f"✅ Successfully inserted {len(data_dict)} records into MongoDB!")

# 5️⃣ Verify by printing a few rows
for doc in collection.find().limit(5):
    print(doc)