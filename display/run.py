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

    return render_template(
        "index.html",
        data=data,
        borough=borough,
        cuisine=cuisine,
        grade=grade
    )


# --------------------------
# Run App
# --------------------------
if __name__ == "__main__":
    app.run(debug=True, port=4205)
