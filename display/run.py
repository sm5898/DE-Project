import os
import sys
import logging

logger = logging.getLogger("py4j")
logger.setLevel(logging.ERROR)

os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"
# os.environ["HADOOP_OPTIONAL_TOOLS"] = ""

logging.getLogger('werkzeug').setLevel(logging.INFO)
os.environ["PYSPARK_SILENCE_STDOUT"] = "1"
os.environ["PYSPARK_SILENCE_SCALA_STDOUT"] = "1"

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
# os.environ['HADOOP_HOME'] = 'C:\\hadoop'

from flask import Flask, render_template, request
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, trim
import json
import pathlib
from pyspark.sql.functions import collect_list, struct, first, col
from pyspark.sql import functions as F
import datetime

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
    # spark.sparkContext.setSystemProperty("hadoop.home.dir", "C:\\hadoop")
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

# Convert date strings to actual date objects for sorting
df = (
    df.withColumn("INSPECTION DATE", F.to_date(col("INSPECTION DATE"), "dd-MM-yyyy"))
      .withColumn("GRADE DATE", F.to_date(col("GRADE DATE"), "dd-MM-yyyy"))
)

windowed = df.orderBy(
    F.col("INSPECTION DATE").desc_nulls_last(),
    F.col("GRADE DATE").desc_nulls_last()
)

df = (
    windowed.groupBy("CAMIS")
    .agg(
        F.first("DBA").alias("DBA"),
        F.first("BORO").alias("BORO"),
        F.first("BUILDING").alias("BUILDING"),
        F.first("STREET").alias("STREET"),
        F.first("ZIPCODE").alias("ZIPCODE"),
        F.first("PHONE").alias("PHONE"),
        F.first("CUISINE DESCRIPTION").alias("CUISINE DESCRIPTION"),
        F.first("GRADE", ignorenulls=True).alias("GRADE"),
        F.first("GRADE DATE", ignorenulls=True).alias("GRADE DATE"),
        F.first("NTA").alias("NTA"),
        F.first("Latitude").alias("Latitude"),
        F.first("Longitude").alias("Longitude"),
        F.collect_list(
            F.struct(
                "INSPECTION DATE", "ACTION", "VIOLATION CODE",
                "VIOLATION DESCRIPTION", "CRITICAL FLAG",
                "SCORE", "INSPECTION TYPE", "GRADE", "GRADE DATE"
            )
        ).alias("VIOLATIONS")
    )
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
    import os, datetime, json
    import pandas as pd
    from pyspark.sql import functions as F

    borough = request.args.get("borough")
    cuisine = request.args.get("cuisine")
    grade = request.args.get("grade")
    neighborhood = request.args.get("neighborhood")
    zipcode = request.args.get("zipcode")
    search = request.args.get("search")

    filtered = df

    # --- Filtering logic ---
    if borough:
        filtered = filtered.filter(F.upper(F.col("BORO")) == borough.upper())
    if cuisine:
        filtered = filtered.filter(F.upper(F.col("CUISINE DESCRIPTION")) == cuisine.upper())
    if grade:
        if grade.upper() == "Z":
            filtered = filtered.filter(
                (F.upper(F.col("GRADE")) == "Z") | F.col("GRADE").isNull()
            )
        else:
            filtered = filtered.filter(F.upper(F.col("GRADE")) == grade.upper())

    if search:
        search = search.strip().upper()
        nta_codes = []

        # --- Load JSON Mapping ---
        nta_path = "display/data/nta_mapping.json"
        # if os.path.exists(nta_path):
        with open(nta_path, "r", encoding="utf-8") as f:
            nta_json = json.load(f)

        # Reverse map to find NTA codes from neighborhood names
        for code, name in nta_json.items():
            if search in name.upper():
                nta_codes.append(code.upper())

        # --- Apply filter in order of specificity ---
        if search.isdigit():
            # ZIP search
            filtered = filtered.filter(F.col("ZIPCODE").cast("string") == search)
        elif nta_codes:
            # NTA-based neighborhood search
            filtered = filtered.filter(F.upper(F.col("NTA")).isin(nta_codes))
        else:
            # Broad search: name, street, borough, cuisine
            filtered = filtered.filter(
                F.upper(F.col("DBA")).contains(search) |
                F.upper(F.col("STREET")).contains(search) |
                F.upper(F.col("BORO")).contains(search) |
                F.upper(F.col("CUISINE DESCRIPTION")).contains(search)
            )

    #  impleent neighborhood and zip filter if needed
    # if neighborhood:
    #     nta_path = os.path.join("data", "nta_mapping.csv")
    #     if os.path.exists(nta_path):
    #         neighborhood_map = pd.read_csv(nta_path)
    #         code = neighborhood_map.loc[
    #             neighborhood_map["Neighborhood"].str.upper() == neighborhood.upper(),
    #             "NTA Code"
    #         ]
    #         if not code.empty:
    #             filtered = filtered.filter(F.upper(F.col("NTA")) == code.iloc[0].upper())

    # if zipcode:
    #     filtered = filtered.filter(F.col("ZIPCODE") == zipcode)

    # --- Convert to Pandas (fixes .get error) ---
    pdf = filtered.limit(200).toPandas()

    # --- Replace grade with latest inspection grade ---
    for idx, row in pdf.iterrows():
        violations = row.get("VIOLATIONS", [])
        if isinstance(violations, (list, tuple)):
            # Convert PySpark Row → dict
            violations = [v.asDict() if hasattr(v, "asDict") else v for v in violations]

            latest = max(
                (v for v in violations if isinstance(v, dict) and v.get("GRADE")),
                key=lambda v: v.get("GRADE DATE") or "",
                default=None
            )
            if latest and latest.get("GRADE"):
                pdf.at[idx, "GRADE"] = latest["GRADE"]

    # --- Clean dates for JSON ---
    def clean_dates(obj):
        if isinstance(obj, (pd.Timestamp, datetime.date)):
            return obj.strftime("%d-%m-%Y")
        elif isinstance(obj, list):
            return [clean_dates(x) for x in obj]
        elif isinstance(obj, dict):
            return {k: clean_dates(v) for k, v in obj.items()}
        else:
            return obj

    data = clean_dates(pdf.to_dict(orient="records"))

    # --- Map Pins ---
    borough_centroids = {
        "MANHATTAN": (40.7831, -73.9712),
        "QUEENS": (40.7282, -73.7949),
        "BROOKLYN": (40.6782, -73.9442),
        "BRONX": (40.8448, -73.8648),
        "STATEN ISLAND": (40.5795, -74.1502),
    }

    pins = []
    for _, row in pdf.iterrows():
        lat, lng = None, None
        for lat_key in ("Latitude", "LAT", "lat", "latitude"):
            if lat_key in pdf.columns and pd.notna(row.get(lat_key)):
                lat = row.get(lat_key)
                break
        for lng_key in ("Longitude", "LNG", "lng", "longitude"):
            if lng_key in pdf.columns and pd.notna(row.get(lng_key)):
                lng = row.get(lng_key)
                break
        if lat is None or lng is None:
            b = (row.get("BORO") or "").upper()
            if b in borough_centroids:
                lat, lng = borough_centroids[b]
        if lat is None or lng is None:
            continue
        pins.append({
            "lat": float(lat),
            "lng": float(lng),
            "name": row.get("DBA") or "",
            "cuisine": row.get("CUISINE DESCRIPTION") or "",
            "grade": row.get("GRADE") or "",
            "camis": row.get("CAMIS") or ""
        })

    safe_data = json.loads(json.dumps(data, default=str))
    safe_pins = json.loads(json.dumps(pins, default=str))

    return render_template(
        "index.html",
        data=safe_data,
        pins=safe_pins,
        borough=borough,
        cuisine=cuisine,
        grade=grade,
        neighborhood=neighborhood,
        zipcode=zipcode,
        search=search
    )

@app.route("/restaurant/<camis>")
def restaurant_detail(camis):
    from pyspark.sql import functions as F
    import math, datetime

    try:
        # --- Match record by CAMIS ---
        restaurant_df = (
            df.filter(F.col("CAMIS") == int(camis))
              .select(
                  "CAMIS", "DBA", "BORO", "BUILDING", "STREET", "ZIPCODE", "PHONE",
                  "CUISINE DESCRIPTION", "GRADE", "GRADE DATE",
                  "Latitude", "Longitude", "VIOLATIONS"
              )
              .limit(1)
              .toPandas()
        )

        # --- Handle missing data ---
        if restaurant_df.empty:
            print(f"[WARN] No restaurant found for CAMIS {camis}")
            return render_template("restaurant_detail.html", restaurant=None)

        restaurant = restaurant_df.to_dict(orient="records")[0]

        # --- Extract latest inspection from nested VIOLATIONS ---
        violations = restaurant.get("VIOLATIONS", [])
        latest_inspection = None

        if violations and isinstance(violations, list):
            valid_inspections = [
                v for v in violations
                if isinstance(v, dict) and v.get("INSPECTION DATE") is not None
            ]
            if valid_inspections:
                latest_inspection = sorted(
                    valid_inspections,
                    key=lambda x: x.get("INSPECTION DATE") or "",
                    reverse=True
                )[0]

        # --- Merge latest inspection fields ---
        if latest_inspection:
            for field in [
                "INSPECTION DATE", "INSPECTION TYPE", "ACTION",
                "SCORE", "CRITICAL FLAG", "GRADE", "GRADE DATE"
            ]:
                if field in latest_inspection and latest_inspection[field] is not None:
                    restaurant[field] = latest_inspection[field]

        # --- Use latest grade if available ---
        if latest_inspection and latest_inspection.get("GRADE"):
            restaurant["GRADE"] = latest_inspection["GRADE"]

        # --- Data sanitization ---
        def clean(v):
            if v is None or (isinstance(v, float) and math.isnan(v)):
                return None
            if isinstance(v, (datetime.date, datetime.datetime)):
                return v.strftime("%Y-%m-%d")
            return v

        restaurant = {k: clean(v) for k, v in restaurant.items()}

        # --- Grade image selection ---
        grade_val = restaurant.get("GRADE", "N/A")
        grade_map = {
            "A": "A.jpeg",
            "B": "B.jpeg",
            "C": "C.jpeg",
            "Z": "pending.jpeg",
            "N/A": "pending.jpeg",
            None: "pending.jpeg"
        }
        grade_img = grade_map.get(str(grade_val).upper(), "pending.jpeg")

        print(f"[INFO] Displaying details for {restaurant.get('DBA')} ({camis}) - Grade: {grade_val}")

        return render_template(
            "restaurant_detail.html",
            restaurant=restaurant,
            grade_img=grade_img
        )

    except Exception as e:
        print("Error in /restaurant route:", e)
        return render_template("restaurant_detail.html", restaurant=None)


# --------------------------
# Run App
# --------------------------
if __name__ == "__main__":
    # NEVER use debug=True when Spark is in the app
    app.run(debug=False, port=5027)