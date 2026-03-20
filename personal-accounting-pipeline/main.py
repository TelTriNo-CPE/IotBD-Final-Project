from pyspark.sql import SparkSession
from src.etl.schemas import transaction_schema, category_schema, merchant_schema
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("Personal Accounting Pipeline") \
    .master("local[*]") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.host", "127.0.0.1") \
    .getOrCreate()

df_transactions = spark.read.csv(
    "data/transactions.csv",
    header=True,
    schema=transaction_schema
)

df_categories = spark.read.csv(
    "data/categories.csv",
    header=True,
    schema=category_schema
)

df_merchants = spark.read.csv(
    "data/merchants.csv",
    header=True,
    schema=merchant_schema
)

# df_transactions.show(5)
# df_transactions.printSchema()

# total_transactions = df_transactions.count()
# print("Total transactions:", total_transactions)

# unique_members = df_transactions.select("member_id").distinct().count()
# unique_merchants = df_transactions.select("merchant_id").distinct().count()
# unique_categories = df_transactions.select("category_id").distinct().count()

# print("Members:", unique_members)
# print("Merchants:", unique_merchants)
# print("Categories:", unique_categories)


# null_amount = df_transactions.filter(
#     col("amount").isNull() | (col("amount") == "")
# ).count()

# print("Null/Empty amount rows:", null_amount)



# min_date = df_transactions.select("date").orderBy("date").first()[0]
# max_date = df_transactions.select("date").orderBy(col("date").desc()).first()[0]

# print("Min date:", min_date)
# print("Max date:", max_date)



# RAW layer
df_transactions.write.mode("overwrite").parquet("output/raw/transactions")
df_categories.write.mode("overwrite").parquet("output/raw/categories")
df_merchants.write.mode("overwrite").parquet("output/raw/merchants")

#STAGED layer (clean data)
from src.etl.validations import filter_valid_transactions

df_clean = filter_valid_transactions(df_transactions)

df_clean.write.mode("overwrite").parquet("output/staged/transactions")


#ANALYTICS (enriched)
from src.etl.transformations import enrich_with_lookups

df_enriched = enrich_with_lookups(df_clean, df_categories, df_merchants)

df_enriched.write.mode("overwrite").parquet("output/analytics/enriched_transactions")


# Part 5 — Analytics & Insight
from pyspark.sql.functions import col, sum, avg, year, month
df_monthly = df_enriched \
    .withColumn("year", year(col("date"))) \
    .withColumn("month", month(col("date"))) \
    .groupBy("year", "month", "category_name") \
    .agg(sum(col("amount").cast("double")).alias("total_amount"))

df_monthly.write.mode("overwrite").parquet("output/analytics/monthly_by_category")

df_yearly_member = df_enriched \
    .withColumn("year", year(col("date"))) \
    .groupBy("year", "member_id") \
    .agg(sum(col("amount").cast("double")).alias("total_amount"))

df_yearly_member.write.mode("overwrite").parquet("output/analytics/yearly_by_member")


from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.partitionBy("year").orderBy(col("total_amount").desc())

df_top_merchants = df_enriched \
    .withColumn("year", year(col("date"))) \
    .groupBy("year", "merchant_name") \
    .agg(sum(col("amount").cast("double")).alias("total_amount")) \
    .withColumn("rank", row_number().over(window_spec)) \
    .filter(col("rank") <= 10)

df_top_merchants.write.mode("overwrite").parquet("output/analytics/top_merchants_by_year")


df_avg = df_enriched \
    .withColumn("year", year(col("date"))) \
    .groupBy("year") \
    .agg(avg(col("amount").cast("double")).alias("avg_amount"))

df_avg.write.mode("overwrite").parquet("output/analytics/avg_amount_by_year")

orphan_count = df_enriched.filter(col("merchant_name").isNull()).count()
print("Orphan merchant rows:", orphan_count)

df_avg.orderBy("year").show()

df_budget = df_enriched \
    .groupBy("budget_type") \
    .agg(sum(col("amount").cast("double")).alias("total_spending"))

df_budget.show()

total_all = df_budget.agg(sum("total_spending")).collect()[0][0]
print("Total spending:", total_all)

from pyspark.sql.functions import lit

df_budget_percent = df_budget.withColumn(
    "percentage",
    (col("total_spending") / lit(total_all)) * 100
)

df_budget_percent.show()


spark.stop()

