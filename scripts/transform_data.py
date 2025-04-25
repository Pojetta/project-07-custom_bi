from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, current_date, datediff
import pathlib

# Paths
ROOT_DIR = pathlib.Path(__file__).resolve().parent.parent
DATA_PATH = ROOT_DIR / "data" / "raw" / "marketing_data.csv"

# Start Spark session
spark = SparkSession.builder.appName("CustomerTransform").getOrCreate()

# Load original CSV
df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .option("sep", ",")
    .csv(str(DATA_PATH))
)

# Parse Dt_Customer column as date
df = df.withColumn("Dt_Customer", to_date("Dt_Customer", "dd-MM-yyyy"))

# Create new column: Customer_Tenure_Days
df = df.withColumn("Customer_Tenure_Days", datediff(current_date(), "Dt_Customer"))

# Preview
df.select("ID", "Dt_Customer", "Customer_Tenure_Days").show(5)
df.printSchema()
