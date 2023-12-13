from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from collections import Counter
from pyspark.sql.functions import expr
from pyspark.sql.functions import col, sum as spark_sum, count, max as spark_max

conf = SparkConf().setAppName("SparkDataFramesAnalysis")
sc = SparkContext(conf=conf)

spark = SparkSession(sc)

file_path = "startup_data.csv"
header = sc.textFile(file_path).first()
rdd = sc.textFile(file_path).filter(lambda line: line != header).map(lambda line: line.split(';'))

# schema
schema = ["unnamed", "state_code", "latitude", "longitude", "city", "address", "name", "labels",
          "founded_at", "closed_at", "first_funding_at", "last_funding_at", "age_first_funding_year",
          "age_last_funding_year", "age_first_milestone_year", "age_last_milestone_year", "relationships",
          "funding_rounds", "funding_total_usd", "milestones", "state_code.1", "is_CA", "is_NY", "is_MA", 
          "is_TX", "is_otherstate", "category_code", "is_software", "is_web", "is_mobile", "is_enterprise",
          "is_advertising", "is_gamesvideo", "is_ecommerce", "is_biotech", "is_consulting", "is_othercategory",
          "object_id", "has_VC", "has_angel", "has_roundA", "has_roundB", "has_roundC", "has_roundD",
          "avg_participants", "is_top500", "status"]

df = rdd.toDF(schema)

# name of all the companies that is of category software
software_df = df.filter(df["category_code"] == 'software')
software_names_df = software_df.select("name")

print("Name of company that belongs to software category:")
software_names_df.show(truncate=False)

# List of category and number of company that particular category have
category_rdd = df.select("category_code").rdd.map(lambda x: x[0])
category_counts = Counter(category_rdd.collect())

# Convert Counter to DataFrame
category_counts_df = spark.createDataFrame(list(category_counts.items()), ["category_code", "count"])

category_counts_df.show(truncate=False)

category_name_df = df.select("category_code", "name")

# Group by category_code and collect_list of names
grouped_df = category_name_df.groupBy("category_code").agg(expr("collect_list(name) as company_list"))

grouped_df.show(truncate=False)

# Map state counts using Counter
state_counts_rdd = df.select("state_code").rdd.map(lambda x: (x[0], 1))
state_counts = Counter(state_counts_rdd.collect())

# Convert Counter to DataFrame
state_counts_df = spark.createDataFrame(list(state_counts.items()), ["state_code", "count"])

state_counts_df.show(truncate=False)

# Map acquired status counts using Counter
acquired_status_rdd = df.select("status").rdd.map(lambda x: (x[0], 1))
acquired_count = Counter(acquired_status_rdd.collect())

# Convert Counter to DataFrame
acquired_count_df = spark.createDataFrame(list(acquired_count.items()), ["status", "count"])

acquired_count_df.show(truncate=False)

# Map total funding for all states
total_funding_rdd = df.select("funding_total_usd").rdd.map(lambda x: int(x[0]))
total_funding = total_funding_rdd.reduce(lambda x, y: x + y)

# Convert total funding to DataFrame
total_funding_df = spark.createDataFrame([(total_funding,)], ["total_funding_usd"])

total_funding_df.show(truncate=False)

# Map category funding amounts
category_funding_df = df.select("category_code", col("funding_total_usd").cast("double"))

# Group by category_code and calculate average funding
average_funding_per_category_df = category_funding_df.groupBy("category_code")\
    .agg((spark_sum("funding_total_usd") / count("funding_total_usd")).alias("average_funding"))

average_funding_per_category_df.show(truncate=False)

# Map category, company, and funding amounts
category_company_funding_df = df.select("category_code", "name", col("funding_total_usd").cast("double"))

# Group by category_code and find the company with the highest funding
top_funded_company_per_category_df = category_company_funding_df.groupBy("category_code")\
    .agg(spark_max(col("funding_total_usd")).alias("max_funding"))

top_funded_company_per_category_df.show(truncate=False)

spark.stop()