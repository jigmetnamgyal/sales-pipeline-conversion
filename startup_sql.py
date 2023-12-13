from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

conf = SparkConf().setAppName("SparkSQLAnalysis")
sc = SparkContext(conf=conf)

spark = SparkSession(sc)

file_path = "startup_data.csv"
header = sc.textFile(file_path).first()
rdd = sc.textFile(file_path).filter(lambda line: line != header).map(lambda line: line.split(';'))

# Define the schema
schema = ["unnamed", "state_code", "latitude", "longitude", "city", "address", "name", "labels",
          "founded_at", "closed_at", "first_funding_at", "last_funding_at", "age_first_funding_year",
          "age_last_funding_year", "age_first_milestone_year", "age_last_milestone_year", "relationships",
          "funding_rounds", "funding_total_usd", "milestones", "state_code.1", "is_CA", "is_NY", "is_MA", 
          "is_TX", "is_otherstate", "category_code", "is_software", "is_web", "is_mobile", "is_enterprise",
          "is_advertising", "is_gamesvideo", "is_ecommerce", "is_biotech", "is_consulting", "is_othercategory",
          "object_id", "has_VC", "has_angel", "has_roundA", "has_roundB", "has_roundC", "has_roundD",
          "avg_participants", "is_top500", "status"]

df = spark.createDataFrame(rdd, schema=schema)

# Create a temporary view for Spark SQL
df.createOrReplaceTempView("startup_data")

sql_query = "SELECT name FROM startup_data WHERE category_code = 'software'"

software_companies = spark.sql(sql_query)

software_companies.show()

# List of category and number of company with that particular category have
sql_query = "SELECT category_code, COUNT(*) as count FROM startup_data GROUP BY category_code"
category_company_count = spark.sql(sql_query)

category_company_count.show()

# Group all the companies that belongs under each category
sql_query = """
    SELECT category_code, COLLECT_LIST(name) as company_list
    FROM startup_data
    GROUP BY category_code
"""

group_company_by_category = spark.sql(sql_query)

group_company_by_category.show(truncate=False)

# SQL query to get state counts
sql_query = """
    SELECT state_code, COUNT(*) as count
    FROM startup_data
    GROUP BY state_code
"""

state_count = spark.sql(sql_query)

state_count.show(truncate=False)

sql_query = """
    SELECT status, COUNT(*) as count
    FROM startup_data
    WHERE status IN ('acquired', 'closed')
    GROUP BY status
"""

startup_acquired_or_closed = spark.sql(sql_query)

startup_acquired_or_closed.show(truncate=False)

sql_query = """
    SELECT SUM(CAST(funding_total_usd AS DECIMAL)) as total_funding
    FROM startup_data
"""

total_funding = spark.sql(sql_query)

total_funding.show(truncate=False)

sql_query = """
    SELECT category_code, AVG(CAST(funding_total_usd AS DOUBLE)) as avg_funding
    FROM startup_data
    GROUP BY category_code
"""

avg_funding = spark.sql(sql_query)

avg_funding.show(truncate=False)

sql_query = """
    SELECT category_code, name as top_funded_company, CAST(funding_total_usd AS DOUBLE) as funding
    FROM (
        SELECT category_code, name, CAST(funding_total_usd AS DOUBLE), 
               ROW_NUMBER() OVER (PARTITION BY category_code ORDER BY CAST(funding_total_usd AS DOUBLE) DESC) as rank
        FROM startup_data
    ) ranked
    WHERE rank = 1
"""

result = spark.sql(sql_query)

result.show(truncate=False)

# Stop the SparkContext
sc.stop()
