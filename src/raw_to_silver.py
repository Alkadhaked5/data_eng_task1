import os

os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["hadoop.home.dir"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ["PATH"]


from pyspark.sql import SparkSession


# Create Spark session
spark = (
    SparkSession.builder
    .appName("DummyJSON_Raw_to_Silver")
    .getOrCreate()
)

# Path to raw JSON file
raw_path = "data/raw/products.json"

# Read raw JSON
df_raw = (
    spark.read
    .option("multiLine", True)
    .json(raw_path)
)

print("Spark Version:", spark.version)
print("Raw Data Schema:")
df_raw.printSchema()

print("Raw Data Count:", df_raw.count())
print("Number of products:", df_raw.selectExpr("size(products)").collect()[0][0])
from pyspark.sql.functions import explode, col

df_products = df_raw.select(
    explode(col("products")).alias("product")
)

print("Product rows after explode:", df_products.count())
print("Product Struct Schema:")
df_products.printSchema()

df_silver = df_products.select(
    "product.*"
)
from pyspark.sql.functions import col

df_silver = df_silver.select(
    "*",
    col("dimensions.width").alias("width"),
    col("dimensions.height").alias("height"),
    col("dimensions.depth").alias("depth"),
    col("meta.barcode").alias("barcode"),
    col("meta.createdAt").alias("created_at"),
    col("meta.updatedAt").alias("updated_at")
)

print("Silver schema after flattening:")
df_silver.printSchema()

for old_col in df_silver.columns:
    new_col = old_col.lower()
    df_silver = df_silver.withColumnRenamed(old_col, new_col)

print("Standardized columns:")
print(df_silver.columns)
df_silver = df_silver.drop(
    "dimensions",
    "meta"
)

print("Columns after removing unnecessary nested fields:")
print(df_silver.columns)
from pyspark.sql.functions import to_timestamp

df_silver = df_silver.withColumn(
    "created_at",
    to_timestamp("created_at")
).withColumn(
    "updated_at",
    to_timestamp("updated_at")
)

print("Schema after type casting:")
df_silver.printSchema()


print("Silver columns:")
print(df_silver.columns)

print("Silver row count:", df_silver.count())
from pyspark.sql.functions import col, sum

print("Null values in each column:")

null_counts = df_silver.select(
    *[
        sum(col(c).isNull().cast("int")).alias(c)
        for c in df_silver.columns
    ]
)

null_counts.show()

print("Duplicate product IDs:")

duplicate_ids = (
    df_silver
    .groupBy("id")
    .count()
    .filter("count > 1")
)

duplicate_ids.show()

print("Number of duplicate IDs:", duplicate_ids.count())

print("Invalid price values:")

invalid_price = df_silver.filter(
    col("price") <= 0
)

print("Invalid price count:", invalid_price.count())
print("Invalid rating values:")

invalid_rating = df_silver.filter(
    (col("rating") < 0) | (col("rating") > 5)
)

print("Invalid rating count:", invalid_rating.count())
print("Invalid stock values:")

invalid_stock = df_silver.filter(
    col("stock") < 0
)

print("Invalid stock count:", invalid_stock.count())

print("Invalid discount percentage values:")

invalid_discount = df_silver.filter(
    col("discountpercentage") < 0
)

print("Invalid discount percentage count:", invalid_discount.count())

print("Invalid minimum order quantity values:")

invalid_min_order = df_silver.filter(
    col("minimumorderquantity") <= 0
)

print(
    "Invalid minimum order quantity count:",
    invalid_min_order.count()
)

print("Invalid weight values:")

invalid_weight = df_silver.filter(
    col("weight") < 0
)

print("Invalid weight count:", invalid_weight.count())
print("Invalid timestamp values:")

invalid_timestamps = df_silver.filter(
    col("updated_at") < col("created_at")
)

print(
    "Invalid timestamp count:",
    invalid_timestamps.count()
)
print("Unexpected availability status values:")

valid_status = [
    "In Stock",
    "Low Stock",
    "Out of Stock"
]

invalid_status = df_silver.filter(
    ~col("availabilitystatus").isin(valid_status)
)

print(
    "Unexpected availability status count:",
    invalid_status.count()
)
print("Unexpected return policy values:")

valid_return_policies = [
    "7 days return policy",
    "30 days return policy",
    "60 days return policy",
    "90 days return policy",
    "No return policy"
]
invalid_return_policy = df_silver.filter(
    ~col("returnpolicy").isin(valid_return_policies)
)

print(
    "Unexpected return policy count:",
    invalid_return_policy.count()
)
print("Actual return policy values:")

df_silver.groupBy("returnpolicy").count().show(truncate=False)

print("Availability and stock consistency check:")

inconsistent_availability = df_silver.filter(
    ((col("availabilitystatus") == "Out of Stock") & (col("stock") > 0)) |
    ((col("availabilitystatus") == "In Stock") & (col("stock") <= 0))
)

print(
    "Availability-stock inconsistency count:",
    inconsistent_availability.count()
)

print("Writing Silver data to Parquet...")

silver_path = "data/silver/products"

df_silver.write.mode("overwrite").parquet(silver_path)

print("Silver Parquet written successfully!")


spark.stop()