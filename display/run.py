from flask import Flask, render_template, request
from pymongo import MongoClient

# --------------------------
# Flask App Setup
# --------------------------
app = Flask(__name__, template_folder="templates")

# --------------------------
# MongoDB Connection
# --------------------------
client = MongoClient("mongodb://localhost:27017/")
db = client["cleanbite"]
restaurants = db["restaurants"]

# --------------------------
# Routes
# --------------------------

@app.route('/')
def home():
    """Landing Page"""
    return render_template('home.html')


@app.route('/restaurants')
def view_restaurants():
    """Display restaurant data with optional filters"""
    borough = request.args.get('borough')
    cuisine = request.args.get('cuisine')

    query = {}
    if borough:
        query['BORO'] = borough
    if cuisine:
        query['CUISINE_DESCRIPTION'] = cuisine

    # Retrieve up to 100 rows for readability
    data = list(restaurants.find(query, {'_id': 0}).limit(100))

    return render_template('index.html', data=data, borough=borough, cuisine=cuisine)


# --------------------------
# Run App
# --------------------------
if __name__ == '__main__':
    app.run(debug=True)