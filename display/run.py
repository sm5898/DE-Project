from flask import Flask, render_template, request
from pymongo import MongoClient
import json # REQUIRED: To enable the 'tojson' filter

# --------------------------
# Flask App Setup
# --------------------------
app = Flask(__name__, template_folder="templates")

# FIX: Register the json.dumps function as the 'tojson' filter for JavaScript
app.jinja_env.filters['tojson'] = json.dumps 

# --------------------------
# MongoDB Connection
# --------------------------
# Ensure MongoDB is running on the default port
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
    """Display restaurant data with optional filters and pass it to the map"""
    
    # --- 1. Retrieve all parameters from the URL ---
    borough = request.args.get('borough')
    cuisine = request.args.get('cuisine')
    grade = request.args.get('grade') # <--- NEW: Retrieve the grade filter
    
    query = {}
    
    # --- 2. Build the MongoDB Query based on parameters ---
    
    # Borough filter (case-insensitive regex)
    if borough:
        query['BORO'] = {'$regex': f'^{borough}$', '$options': 'i'}
    
    # Cuisine filter (case-insensitive regex on the correct field name)
    if cuisine:
        # Field name must use a space: 'CUISINE DESCRIPTION'
        query['CUISINE DESCRIPTION'] = {'$regex': f'^{cuisine}$', '$options': 'i'}

    # Grade filter (Exact match)
    if grade:
        if grade == 'Z':
            # Handle the 'Pending' case. In the dataset, this is usually 'Z' or null/missing.
            # We query for both 'Z' and any documents where the GRADE field is missing ($exists: false)
            query['GRADE'] = {
                '$in': ['Z', None] # Look for 'Z' or explicit None/null
            }
        else:
            # Query for exact match on 'A', 'B', or 'C'
            query['GRADE'] = grade

    # --- 3. Execute the Query ---
    
    # Retrieve up to 100 rows
    # The map needs the Latitude and Longitude fields
    data = list(restaurants.find(query, {'_id': 0}).limit(100))

    # --- 4. Render the Template ---
    return render_template(
        'index.html', 
        data=data, 
        borough=borough, 
        cuisine=cuisine, 
        grade=grade
    )


# --------------------------
# Run App
# --------------------------
if __name__ == '__main__':
    app.run(debug=True)