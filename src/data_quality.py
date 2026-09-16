import os

os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["hadoop.home.dir"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ["PATH"]

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when

spark = (
    SparkSession.builder
    .appName("Data_Quality_Check")
    .getOrCreate()
)

silver_path = "data/silver/products"

df_silver = spark.read.parquet(silver_path)

print("Silver Data Count:", df_silver.count())

print("\nSilver Data Schema:")
df_silver.printSchema()

print("\nNull Values in Each Column:")
null_counts = df_silver.select(
    [
        count(when(col(c).isNull(), c)).alias(c)
        for c in df_silver.columns
    ]
)
null_counts.show()

# YAHAN duplicate check add karo
print("\nDuplicate Product IDs:")

duplicate_ids = (
    df_silver
    .groupBy("id")
    .count()
    .filter(col("count") > 1)
)

duplicate_ids.show()

duplicate_id_count = duplicate_ids.count()

print("Number of duplicate IDs:", duplicate_id_count)

print("\nInvalid Price Values:")

invalid_price_count = (
    df_silver
    .filter(col("price") <= 0)
    .count()
)

print("Invalid price count:", invalid_price_count)

print("\nInvalid Rating Values:")

invalid_rating_count = (
    df_silver
    .filter((col("rating") < 0) | (col("rating") > 5))
    .count()
)

print("Invalid rating count:", invalid_rating_count)

print("\nInvalid Stock Values:")

invalid_stock_count = (
    df_silver
    .filter(col("stock") < 0)
    .count()
)

print("Invalid stock count:", invalid_stock_count)

print("\nInvalid Discount Percentage Values:")

invalid_discount_count = (
    df_silver
    .filter((col("discountpercentage") < 0) | (col("discountpercentage") > 100))
    .count()
)

print("Invalid discount percentage count:", invalid_discount_count)

print("\nInvalid Minimum Order Quantity Values:")

invalid_min_order_count = (
    df_silver
    .filter(col("minimumorderquantity") <= 0)
    .count()
)

print("Invalid minimum order quantity count:", invalid_min_order_count)

print("\nInvalid Weight Values:")

invalid_weight_count = (
    df_silver
    .filter(col("weight") < 0)
    .count()
)

print("Invalid weight count:", invalid_weight_count)

print("\nInvalid Timestamp Values:")

invalid_timestamp_count = (
    df_silver
    .filter(col("updated_at") < col("created_at"))
    .count()
)

print("Invalid timestamp count:", invalid_timestamp_count)

print("\nUnexpected Availability Status Values:")

valid_availability = [
    "In Stock",
    "Low Stock",
    "Out of Stock"
]

unexpected_availability_count = (
    df_silver
    .filter(~col("availabilitystatus").isin(valid_availability))
    .count()
)

print("Unexpected availability status count:", unexpected_availability_count)

print("\nUnexpected Return Policy Values:")

valid_return_policies = [
    "7 days return policy",
    "30 days return policy",
    "60 days return policy",
    "90 days return policy",
    "No return policy"
]

unexpected_return_policy_count = (
    df_silver
    .filter(~col("returnpolicy").isin(valid_return_policies))
    .count()
)

print("Unexpected return policy count:", unexpected_return_policy_count)

print("\nAvailability and Stock Consistency Check:")

availability_stock_issues = (
    df_silver.filter(
        ((col("availabilitystatus") == "Out of Stock") & (col("stock") > 0))
        |
        ((col("availabilitystatus") == "In Stock") & (col("stock") <= 0))
    )
)

availability_stock_issue_count = availability_stock_issues.count()

print(
    "Availability-stock inconsistency count:",
    availability_stock_issue_count
)




spark.stop()