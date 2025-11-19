import os
import sys
import logging

logger = logging.getLogger("py4j")
logger.setLevel(logging.ERROR)

os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"
os.environ["HADOOP_OPTIONAL_TOOLS"] = ""

logging.getLogger('werkzeug').setLevel(logging.INFO)
os.environ["PYSPARK_SILENCE_STDOUT"] = "1"
os.environ["PYSPARK_SILENCE_SCALA_STDOUT"] = "1"

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['HADOOP_HOME'] = 'C:\\hadoop'

from flask import Flask, render_template, request
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, trim
import json
import pathlib

# -------------------------------------------------
# FLASK SETUP
# -------------------------------------------------
app = Flask(__name__, template_folder="templates")
app.jinja_env.filters['tojson'] = json.dumps

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))      # C:/.../DE-Project/display
DE_PROJECT = os.path.dirname(PROJECT_ROOT)                     # C:/.../DE-Project
JARS_DIR = os.path.join(PROJECT_ROOT, "jars")                  # C:/.../DE-Project/display/jars

# Collect all JAR files in jars/ directory
jar_files = []
for f in os.listdir(JARS_DIR):
    if f.endswith(".jar"):
        file_path = os.path.join(JARS_DIR, f)
        uri = pathlib.Path(file_path).absolute().as_uri()  # <-- Converts to C:/....
        jar_files.append(uri)

jars_string = ",".join(jar_files)


# -------------------------------------------------
# SPARK INITIALIZATION — FIXED
# -------------------------------------------------

def create_spark():
    """
    Create SparkSession only ONCE.
    Prevent Ivy from resolving dependencies repeatedly.
    Reduce Spark logs.
    Prevent double creation due to Flask auto-reload.
    """

    # Reduce Spark logging
    os.environ["PYSPARK_SUBMIT_ARGS"] = "--conf spark.ui.showConsoleProgress=false pyspark-shell"
    spark = (
        SparkSession.builder
            .appName("CleanBite")
            # use **local jar** to avoid Ivy downloading every run
            .config("spark.jars", jars_string)
            .config("spark.mongodb.read.connection.uri", "mongodb://127.0.0.1")
            .config("spark.mongodb.write.connection.uri", "mongodb://127.0.0.1")
            .config("spark.mongodb.read.database", "cleanbite")
            .config("spark.mongodb.write.database", "cleanbite")
            .config("spark.mongodb.read.collection", "restaurants")
            .config("spark.mongodb.write.collection", "restaurants")
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "4g")
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    spark.sparkContext.setSystemProperty("hadoop.home.dir", "C:\\hadoop")
    return spark

# Create Spark globally only once
spark = create_spark()

print("Using Apache Spark Version:", spark.version)


# -------------------------------------------------
# LOAD & CLEAN DATA — FIXED
# -------------------------------------------------

df = (
    spark.read.format("mongodb")
        .load()
        .withColumn("BORO", trim(upper(col("BORO"))))
        .withColumn("CUISINE DESCRIPTION", trim(upper(col("CUISINE DESCRIPTION"))))
        .withColumn("GRADE", trim(upper(col("GRADE"))))
)

df.limit(1).count()

# -------------------------------------------------
# ROUTES
# -------------------------------------------------

@app.route("/")
def home():
    return render_template("home.html")


@app.route("/restaurants")
def view_restaurants():

    borough = request.args.get("borough")
    cuisine = request.args.get("cuisine")
    grade = request.args.get("grade")

    filtered = df

    if borough:
        filtered = filtered.filter(upper(col("BORO")) == borough.upper())

    if cuisine:
        filtered = filtered.filter(upper(col("CUISINE DESCRIPTION")) == cuisine.upper())

    if grade:
        if grade.upper() == "Z":
            filtered = filtered.filter(
                (upper(col("GRADE")) == "Z") | col("GRADE").isNull()
            )
        else:
            filtered = filtered.filter(upper(col("GRADE")) == grade.upper())

    data = filtered.limit(100).toPandas().to_dict(orient="records")

    # Build pins for the map from the same filtered dataframe so map and list stay in sync
    pdf = filtered.limit(200).toPandas()

    # borough centroids for quick fallback (approx)
    borough_centroids = {
        'MANHATTAN': (40.7831, -73.9712),
        'QUEENS': (40.7282, -73.7949),
        'BROOKLYN': (40.6782, -73.9442),
        'BRONX': (40.8448, -73.8648),
        'STATEN ISLAND': (40.5795, -74.1502)
    }

    pins = []
    for _, row in pdf.iterrows():
        lat = None
        lng = None
        # try common lat/lng fields
        for lat_key in ('lat', 'LAT', 'latitude', 'Latitude'):
            if lat_key in pdf.columns and pd.notna(row.get(lat_key)):
                lat = row.get(lat_key); break
        for lng_key in ('lng', 'LNG', 'longitude', 'Longitude'):
            if lng_key in pdf.columns and pd.notna(row.get(lng_key)):
                lng = row.get(lng_key); break

        # If no coords, fallback to borough centroid
        if lat is None or lng is None:
            b = (row.get('BORO') or row.get('boro') or '')
            bc = borough_centroids.get(str(b).upper())
            if bc:
                lat, lng = bc

        if lat is None or lng is None:
            continue

        pins.append({
            'lat': float(lat),
            'lng': float(lng),
            'name': row.get('DBA') or row.get('dba') or row.get('name') or '',
            'cuisine': row.get('CUISINE DESCRIPTION') or row.get('CUISINE_DESCRIPTION') or row.get('cuisine') or '',
            'grade': row.get('GRADE') or row.get('grade') or ''
        })

    return render_template(
        "index.html",
        data=data,
        pins=pins,
        borough=borough,
        cuisine=cuisine,
        grade=grade
    )

@app.route("/restaurant/<camis>")
def restaurant_detail(camis):
    """
    Display details for a single restaurant using its CAMIS (unique restaurant ID).
    """
    single_df = df.filter(col("CAMIS") == camis)
    data = single_df.limit(1).toPandas().to_dict(orient="records")

    if not data:
        return f"<h3>No restaurant found with CAMIS {camis}</h3>"

    restaurant = data[0]
    return render_template("restaurant_detail.html", r=restaurant)




# --------------------------
# Run App
# --------------------------
if __name__ == "__main__":
    # NEVER use debug=True when Spark is in the app
    app.run(debug=False, port=5027)