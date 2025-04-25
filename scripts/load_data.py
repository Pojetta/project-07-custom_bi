from pyspark.sql import SparkSession
import pathlib

# Paths
ROOT_DIR = pathlib.Path(__file__).resolve().parent.parent
DATA_PATH = ROOT_DIR / "data" / "raw" / "marketing_data.csv"

# Spark session
spark = SparkSession.builder.appName("CustomerBI").getOrCreate()

# Load CSV with correct delimiter
df = spark.read.option("header", True) \
    .option("inferSchema", True) \
    .option("sep", ",") \
    .csv(str(DATA_PATH))

# Preview
df.show(5)
df.printSchema()

df = spark.read.option("header", True).option("inferSchema", True).csv(str(DATA_PATH))

# Clean column names (optional)
for colname in df.columns:
    df = df.withColumnRenamed(colname, colname.strip().replace(" ", "_"))

df.show(5)
df.printSchema()
