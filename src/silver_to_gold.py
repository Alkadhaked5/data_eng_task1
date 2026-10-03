import os

os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["hadoop.home.dir"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ["PATH"]

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Silver_to_Gold")
    .getOrCreate()
)

silver_path = "data/silver/products"

df_silver = spark.read.parquet(silver_path)

print("Silver Data Count:", df_silver.count())

print("\nSilver Data Schema:")
df_silver.printSchema()

from pyspark.sql.functions import col, count, sum as spark_sum, when

# KPI-01: Inventory Value

df_product_kpi = df_silver.withColumn(
    "inventory_value",
    col("price") * col("stock")
)

print("\nProduct-level Inventory Value:")
df_product_kpi.select(
    "id",
    "title",
    "category",
    "brand",
    "price",
    "stock",
    "inventory_value"
).show(10, truncate=False)

df_category_kpi = (
    df_product_kpi
    .groupBy("category")
    .agg(
        spark_sum("stock").alias("total_stock"),
        spark_sum("inventory_value").alias("inventory_value")
    )
)

print("\nCategory-level Inventory Value:")
df_category_kpi.show(truncate=False)

overall_inventory_value = (
    df_product_kpi
    .agg(
        spark_sum("inventory_value").alias("total_inventory_value")
    )
)

print("\nOverall Inventory Value:")
overall_inventory_value.show()


# KPI-02: Discounted Inventory Value

df_product_kpi = df_product_kpi.withColumn(
    "discounted_inventory_value",
    col("price")
    * (1 - col("discountpercentage") / 100)
    * col("stock")
)

print("\nProduct-level Discounted Inventory Value:")
df_product_kpi.select(
    "id",
    "title",
    "price",
    "discountpercentage",
    "stock",
    "inventory_value",
    "discounted_inventory_value"
).show(10, truncate=False)

df_category_kpi = (
    df_product_kpi
    .groupBy("category")
    .agg(
        spark_sum("stock").alias("total_stock"),
        spark_sum("inventory_value").alias("inventory_value"),
        spark_sum("discounted_inventory_value").alias(
            "discounted_inventory_value"
        )
    )
)

print("\nCategory-level Discounted Inventory Value:")
df_category_kpi.show(truncate=False)

overall_inventory_value = (
    df_product_kpi
    .agg(
        spark_sum("inventory_value").alias("total_inventory_value"),
        spark_sum("discounted_inventory_value").alias(
            "total_discounted_inventory_value"
        )
    )
)

print("\nOverall Inventory Value Comparison:")
overall_inventory_value.show()



# KPI-03: Category Sales Potential

df_category_sales_potential = (
    df_silver
    .groupBy("category")
    .agg(
        count("id").alias("total_products"),
        spark_sum("stock").alias("total_stock"),
        spark_sum(col("price") * col("stock")).alias(
            "sales_potential"
        )
    )
)

print("\nCategory Sales Potential:")
df_category_sales_potential.show(truncate=False)



# KPI-04: Low Stock Rate

low_stock_threshold = 10

df_category_low_stock = (
    df_silver
    .groupBy("category")
    .agg(
        count("*").alias("total_products"),
        count(
            when(col("stock") <= low_stock_threshold, True)
        ).alias("low_stock_products")
    )
    .withColumn(
        "low_stock_rate",
        col("low_stock_products") / col("total_products") * 100
    )
)

print("\nCategory Low Stock Rate:")
df_category_low_stock.show(truncate=False)


# KPI-05: Weighted Average Product Rating

df_category_rating = (
    df_silver
    .groupBy("category")
    .agg(
        spark_sum(col("rating") * col("stock")).alias(
            "weighted_rating_sum"
        ),
        spark_sum("stock").alias("total_stock")
    )
    .withColumn(
        "weighted_rating",
        col("weighted_rating_sum") / col("total_stock")
    )
    .select(
        "category",
        "total_stock",
        "weighted_rating"
    )
)

print("\nCategory Weighted Average Rating:")
df_category_rating.show(truncate=False)



# KPI-06: Brand Discount Impact

df_brand_kpi = (
    df_silver
    .groupBy("brand")
    .agg(
        count("*").alias("product_count"),
        spark_sum("stock").alias("total_stock"),

        spark_sum(
            col("price") * col("stock")
        ).alias("inventory_value"),

        spark_sum(
            col("price")
            * col("stock")
            * col("discountpercentage")
            / 100
        ).alias("discount_amount"),

        spark_sum(
            col("price")
            * (1 - col("discountpercentage") / 100)
            * col("stock")
        ).alias("discounted_inventory_value"),

        spark_sum(
            col("discountpercentage")
        ).alias("total_discount_percentage"),

        spark_sum(
            col("rating")
        ).alias("rating_sum")
    )
    .withColumn(
        "avg_discount",
        col("total_discount_percentage") / col("product_count")
    )
    .withColumn(
        "avg_rating",
        col("rating_sum") / col("product_count")
    )
    .select(
        "brand",
        "product_count",
        "total_stock",
        "inventory_value",
        "discount_amount",
        "discounted_inventory_value",
        "avg_discount",
        "avg_rating"
    )
)

print("\nBrand Discount Impact:")
df_brand_kpi.show(truncate=False)







# KPI-07: Inventory Concentration

total_inventory_value = (
    df_silver
    .agg(
        spark_sum(col("price") * col("stock"))
        .alias("total_inventory_value")
    )
    .collect()[0]["total_inventory_value"]
)

df_category_concentration = (
    df_silver
    .groupBy("category")
    .agg(
        count("*").alias("product_count"),
        spark_sum("stock").alias("total_stock"),
        spark_sum(
            col("price") * col("stock")
        ).alias("inventory_value")
    )
    .withColumn(
        "inventory_concentration_pct",
        col("inventory_value")
        / total_inventory_value
        * 100
    )
)

print("\nCategory Inventory Concentration:")
df_category_concentration.show(truncate=False)




# KPI-08: Product Health Score
# Step 1: Normalize KPI components

from pyspark.sql.functions import min as spark_min, max as spark_max

rating_min = df_silver.agg(
    spark_min("rating")
).collect()[0][0]

rating_max = df_silver.agg(
    spark_max("rating")
).collect()[0][0]

stock_min = df_silver.agg(
    spark_min("stock")
).collect()[0][0]

stock_max = df_silver.agg(
    spark_max("stock")
).collect()[0][0]

discount_min = df_silver.agg(
    spark_min("discountpercentage")
).collect()[0][0]

discount_max = df_silver.agg(
    spark_max("discountpercentage")
).collect()[0][0]

df_health = (
    df_silver
    .withColumn(
        "rating_score",
        (col("rating") - rating_min)
        / (rating_max - rating_min)
    )
    .withColumn(
        "stock_score",
        (col("stock") - stock_min)
        / (stock_max - stock_min)
    )
    .withColumn(
        "discount_score",
        (col("discountpercentage") - discount_min)
        / (discount_max - discount_min)
    )
)

print("\nNormalized Product Health Components:")

df_health.select(
    "id",
    "title",
    "rating",
    "rating_score",
    "stock",
    "stock_score",
    "discountpercentage",
    "discount_score"
).show(10, truncate=False)



# KPI-08: Review Score

from pyspark.sql.functions import explode, sum as spark_sum

df_reviews = (
    df_silver
    .select(
        "id",
        explode("reviews").alias("review")
    )
)

df_review_score = (
    df_reviews
    .groupBy("id")
    .agg(
        count("*").alias("total_reviews"),
        spark_sum(
            when(col("review.rating") >= 4, 1).otherwise(0)
        ).alias("positive_reviews")
    )
    .withColumn(
        "positive_review_pct",
        col("positive_reviews") / col("total_reviews") * 100
    )
)

print("\nProduct Review Score:")

df_review_score.show(10, truncate=False)



# KPI-08: Final Product Health Score

df_health_final = (
    df_health
    .join(
        df_review_score.select(
            "id",
            "positive_review_pct"
        ),
        on="id",
        how="left"
    )
    .withColumn(
        "review_score",
        col("positive_review_pct") / 100
    )
    .withColumn(
        "product_health_score",
        (
            col("rating_score") * 0.40
            + col("review_score") * 0.25
            + col("stock_score") * 0.20
            + col("discount_score") * 0.15
        )
    )
)

print("\nProduct Health Score:")

df_health_final.select(
    "id",
    "title",
    "rating_score",
    "review_score",
    "stock_score",
    "discount_score",
    "product_health_score"
).show(10, truncate=False)



# KPI-09: Review Quality Index

df_review_kpi = (
    df_reviews
    .groupBy("id")
    .agg(
        count("*").alias("total_reviews"),

        spark_sum(
            col("review.rating")
        ).alias("rating_sum"),

        spark_sum(
            when(col("review.rating") == 1, 1).otherwise(0)
        ).alias("one_star_reviews"),

        spark_sum(
            when(col("review.rating") == 2, 1).otherwise(0)
        ).alias("two_star_reviews"),

        spark_sum(
            when(col("review.rating") == 3, 1).otherwise(0)
        ).alias("three_star_reviews"),

        spark_sum(
            when(col("review.rating") == 4, 1).otherwise(0)
        ).alias("four_star_reviews"),

        spark_sum(
            when(col("review.rating") == 5, 1).otherwise(0)
        ).alias("five_star_reviews")
    )
    .withColumn(
        "avg_review_rating",
        col("rating_sum") / col("total_reviews")
    )
    .withColumn(
        "one_star_pct",
        col("one_star_reviews") / col("total_reviews") * 100
    )
    .withColumn(
        "two_star_pct",
        col("two_star_reviews") / col("total_reviews") * 100
    )
    .withColumn(
        "three_star_pct",
        col("three_star_reviews") / col("total_reviews") * 100
    )
    .withColumn(
        "four_star_pct",
        col("four_star_reviews") / col("total_reviews") * 100
    )
    .withColumn(
        "five_star_pct",
        col("five_star_reviews") / col("total_reviews") * 100
    )
    .withColumn(
        "positive_review_pct",
        (
            col("four_star_reviews")
            + col("five_star_reviews")
        )
        / col("total_reviews") * 100
    )
    .select(
        "id",
        "total_reviews",
        "avg_review_rating",
        "one_star_pct",
        "two_star_pct",
        "three_star_pct",
        "four_star_pct",
        "five_star_pct",
        "positive_review_pct"
    )
)

print("\nReview Quality Index:")

df_review_kpi.show(10, truncate=False)



# KPI-10: Commercial Opportunity Score
# Step 1: Normalize components

price_min = df_silver.agg(
    spark_min("price")
).collect()[0][0]

price_max = df_silver.agg(
    spark_max("price")
).collect()[0][0]

df_commercial = (
    df_silver
    .join(
        df_review_kpi.select(
            "id",
            "positive_review_pct"
        ),
        on="id",
        how="left"
    )
    .withColumn(
        "commercial_rating_score",
        (col("rating") - rating_min)
        / (rating_max - rating_min)
    )
    .withColumn(
        "commercial_review_score",
        col("positive_review_pct") / 100
    )
    .withColumn(
        "commercial_discount_score",
        (col("discountpercentage") - discount_min)
        / (discount_max - discount_min)
    )
    .withColumn(
        "commercial_stock_score",
        (col("stock") - stock_min)
        / (stock_max - stock_min)
    )
    .withColumn(
        "price_score",
        (price_max - col("price"))
        / (price_max - price_min)
    )
)

print("\nNormalized Commercial Opportunity Components:")

df_commercial.select(
    "id",
    "title",
    "commercial_rating_score",
    "commercial_review_score",
    "commercial_discount_score",
    "commercial_stock_score",
    "price_score"
).show(10, truncate=False)



# KPI-10: Final Commercial Opportunity Score

df_commercial_final = (
    df_commercial
    .withColumn(
        "commercial_opportunity_score",
        (
            col("commercial_rating_score") * 0.30
            + col("commercial_review_score") * 0.25
            + col("commercial_discount_score") * 0.20
            + col("commercial_stock_score") * 0.15
            + col("price_score") * 0.10
        )
    )
)

print("\nCommercial Opportunity Score:")

df_commercial_final.select(
    "id",
    "title",
    "commercial_rating_score",
    "commercial_review_score",
    "commercial_discount_score",
    "commercial_stock_score",
    "price_score",
    "commercial_opportunity_score"
).show(10, truncate=False)



# Gold Table 1: Product KPI

df_gold_product_kpi = (
    df_health_final
    .join(
    df_review_kpi.select(
        "id",
        "avg_review_rating"
    ),
    on="id",
    how="left"
)
    .join(
        df_commercial_final.select(
            "id",
            "commercial_opportunity_score"
        ),
        on="id",
        how="left"
    )
    .withColumnRenamed("id", "product_id")
    .withColumn(
        "inventory_value",
        col("price") * col("stock")
    )
    .withColumn(
        "discounted_inventory_value",
        col("price")
        * (1 - col("discountpercentage") / 100)
        * col("stock")
    )
    .withColumn(
        "low_stock_flag",
        when(col("stock") <= 10, 1).otherwise(0)
    )
    .select(
        "product_id",
        "category",
        "brand",
        "price",
        "stock",
        "inventory_value",
        "discounted_inventory_value",
        "rating",
        "avg_review_rating",
        "positive_review_pct",
        "low_stock_flag",
        "product_health_score",
        "commercial_opportunity_score"
    )
)

print("\nGold Product KPI:")
df_gold_product_kpi.show(10, truncate=False)

print("Gold Product KPI Count:", df_gold_product_kpi.count())


# Gold Table 2: Category KPI

df_gold_category_kpi = (
    df_silver
    .groupBy("category")
    .agg(
        count("*").alias("product_count"),

        spark_sum("stock").alias("total_stock"),

        spark_sum(
            col("price") * col("stock")
        ).alias("inventory_value"),

        spark_sum(
            col("price")
            * (1 - col("discountpercentage") / 100)
            * col("stock")
        ).alias("discounted_inventory_value"),

        spark_sum("rating").alias("rating_sum"),

        spark_sum(
            col("rating") * col("stock")
        ).alias("weighted_rating_sum"),

        count(
            when(col("stock") <= low_stock_threshold, True)
        ).alias("low_stock_products")
    )
    .withColumn(
        "avg_rating",
        col("rating_sum") / col("product_count")
    )
    .withColumn(
        "weighted_rating",
        col("weighted_rating_sum") / col("total_stock")
    )
    .withColumn(
        "low_stock_rate",
        col("low_stock_products")
        / col("product_count") * 100
    )
    .withColumn(
        "inventory_concentration_pct",
        col("inventory_value")
        / total_inventory_value * 100
    )
    .select(
        "category",
        "product_count",
        "total_stock",
        "inventory_value",
        "discounted_inventory_value",
        "avg_rating",
        "weighted_rating",
        "low_stock_rate",
        "inventory_concentration_pct"
    )
)

print("\nGold Category KPI:")
df_gold_category_kpi.show(24, truncate=False)

print(
    "Gold Category KPI Count:",
    df_gold_category_kpi.count()
)


# Gold Table 3: Brand KPI

df_gold_brand_kpi = (
    df_silver
    .join(
        df_review_kpi.select(
            "id",
            "positive_review_pct"
        ),
        on="id",
        how="left"
    )
    .groupBy("brand")
    .agg(
        count("*").alias("product_count"),

        spark_sum("stock").alias("total_stock"),

        spark_sum(
            col("price") * col("stock")
        ).alias("inventory_value"),

        spark_sum(
            col("price")
            * col("stock")
            * col("discountpercentage") / 100
        ).alias("discount_amount"),

        spark_sum(
            col("price")
            * (1 - col("discountpercentage") / 100)
            * col("stock")
        ).alias("discounted_inventory_value"),

        spark_sum("discountpercentage").alias("total_discount"),

        spark_sum("rating").alias("rating_sum"),

        spark_sum("positive_review_pct").alias(
            "positive_review_pct_sum"
        )
    )
    .withColumn(
        "avg_discount",
        col("total_discount") / col("product_count")
    )
    .withColumn(
        "avg_rating",
        col("rating_sum") / col("product_count")
    )
    .withColumn(
        "review_quality_index",
        col("positive_review_pct_sum")
        / col("product_count")
    )
    .select(
        "brand",
        "product_count",
        "total_stock",
        "inventory_value",
        "discount_amount",
        "discounted_inventory_value",
        "avg_discount",
        "avg_rating",
        "review_quality_index"
    )
)

print("\nGold Brand KPI:")
df_gold_brand_kpi.show(20, truncate=False)

print(
    "Gold Brand KPI Count:",
    df_gold_brand_kpi.count()
)


# Gold Table 4: Review KPI

df_gold_review_kpi = (
    df_review_kpi
    .withColumnRenamed("id", "product_id")
)

print("\nGold Review KPI:")
df_gold_review_kpi.show(20, truncate=False)

print(
    "Gold Review KPI Count:",
    df_gold_review_kpi.count()
)



# Write Gold tables to Parquet

gold_product_path = "data/gold/gold_product_kpi"
gold_category_path = "data/gold/gold_category_kpi"
gold_brand_path = "data/gold/gold_brand_kpi"
gold_review_path = "data/gold/gold_review_kpi"

print("\nWriting Gold tables to Parquet...")

df_gold_product_kpi.write.mode("overwrite").parquet(gold_product_path)

df_gold_category_kpi.write.mode("overwrite").parquet(gold_category_path)

df_gold_brand_kpi.write.mode("overwrite").parquet(gold_brand_path)

df_gold_review_kpi.write.mode("overwrite").parquet(gold_review_path)

print("All Gold tables written successfully!")



# Final Gold Validation

print("\nFinal Gold Validation:")

print(
    "Gold Product KPI:",
    spark.read.parquet(gold_product_path).count()
)

print(
    "Gold Category KPI:",
    spark.read.parquet(gold_category_path).count()
)

print(
    "Gold Brand KPI:",
    spark.read.parquet(gold_brand_path).count()
)

print(
    "Gold Review KPI:",
    spark.read.parquet(gold_review_path).count()
)

spark.stop()
print("\nSpark session stopped successfully!")

spark.stop()
print("\nSpark session stopped successfully!")

