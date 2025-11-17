import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['HADOOP_HOME'] = 'C:\\hadoop'


from flask import Flask, render_template, request
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, trim
import json

# -------------------------------------------------
# FLASK SETUP
# -------------------------------------------------
app = Flask(__name__, template_folder="templates")
app.jinja_env.filters['tojson'] = json.dumps


# -------------------------------------------------
# SPARK + MONGODB CONNECTION
# -------------------------------------------------
MONGO_URI = "mongodb://127.0.0.1/cleanbite.restaurants"

spark = SparkSession \
    .builder \
    .appName("CleanBite") \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/cleanbite.restaurants") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/cleanbite.restaurants") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.13:10.3.0") \
    .config("spark.cores.max", "4") \
    .config('spark.executor.memory', '8G') \
    .config('spark.driver.maxResultSize', '8g') \
    .config('spark.kryoserializer.buffer.max', '512m') \
    .config("spark.driver.cores", "4") \
    .getOrCreate()

sc = spark.sparkContext

print("Using Apache Spark Version", spark.version)

# Load MongoDB restaurants collection into Spark DataFrame
df = spark.read \
    .format("mongodb") \
    .option("spark.mongodb.read.connection.uri", "mongodb://127.0.0.1") \
    .option("spark.mongodb.read.database", "cleanbite") \
    .option("spark.mongodb.read.collection", "restaurants") \
    .load()

# Clean data (uppercase and trim)
df = df.withColumn("BORO", trim(upper(col("BORO"))))
df = df.withColumn("CUISINE DESCRIPTION", trim(upper(col("CUISINE DESCRIPTION"))))
df = df.withColumn("GRADE", trim(upper(col("GRADE"))))


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

    # 1. Borough filter
    if borough:
        filtered = filtered.filter(upper(col("BORO")) == borough.upper())

    # 2. Cuisine filter
    if cuisine:
        filtered = filtered.filter(
            upper(col("CUISINE DESCRIPTION")) == cuisine.upper()
        )

    # 3. Grade filter
    if grade:
        if grade.upper() == "Z":
            filtered = filtered.filter(
                (upper(col("GRADE")) == "Z") | col("GRADE").isNull()
            )
        else:
            filtered = filtered.filter(upper(col("GRADE")) == grade.upper())

    # Convert Spark → Python (dict)
    data = filtered.limit(100).toPandas().to_dict(orient="records")

    return render_template(
        "index.html",
        data=data,
        borough=borough,
        cuisine=cuisine,
        grade=grade
    )


# -------------------------------------------------
# RUN APP
# -------------------------------------------------
if __name__ == "__main__":
    app.run(debug=True)