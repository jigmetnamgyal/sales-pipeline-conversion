from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

conf = SparkConf().setAppName("SparkRDDAnalysis")
sc = SparkContext(conf=conf)

spark = SparkSession(sc)

file_path = "startup_data.csv"
header = sc.textFile(file_path).first()
rdd = sc.textFile(file_path).filter(lambda line: line != header).map(lambda line: line.split(';'))

software_rdd = rdd.filter(lambda x: x[26] == 'software')

software_names = software_rdd.map(lambda row: row[6])

names_list = software_names.collect()

# for name in names_list:
#     print(name)


# List of category and number of company that particular category have
category_rdd = rdd.map(lambda row: row[26])
category_counts = category_rdd.countByValue()
print(category_counts)

# Group all the companies that belongs under each category
category_name_rdd = rdd.map(lambda row: (row[26], row[6]))
grouped_rdd = category_name_rdd.groupBy(lambda x: x[0])

result = grouped_rdd.collect()
for category, companies in result:
    company_list = [company[1] for company in companies]
    print(f"{category}: {company_list}")


state_counts = rdd.map(lambda x: (x[1], 1)).countByKey()
print(f'count of start up by state: {state_counts}')

# Total number of start up that are acquired or closed
acquired_status_rdd = rdd.map(lambda row: row[46])
acquired_count = acquired_status_rdd.countByValue()
print(acquired_count)

# Total number of funding that all the start up got in the US
total_funding = rdd.map(lambda x: int(x[18])).reduce(lambda x, y: x + y)
print(f'{total_funding}$')


# calculate the average funding amount for each category.
category_funding_rdd = rdd.map(lambda row: (row[26], float(row[18])))
grouped_rdd = category_funding_rdd.groupBy(lambda x: x[0])
average_funding_per_category = grouped_rdd.mapValues(lambda values: sum(value[1] for value in values) / len(values))
result = average_funding_per_category.collect()

for category, avg_funding in result:
    print(f"{category}: Average Funding - ${avg_funding:.2f}")



# company with the highest funding amount in each category.
category_company_funding_rdd = rdd.map(lambda row: (row[26], row[6], float(row[18])))
grouped_rdd = category_company_funding_rdd.groupBy(lambda x: x[0])
top_funded_company_per_category = grouped_rdd.mapValues(lambda values: max(values, key=lambda x: x[2]))
result = top_funded_company_per_category.collect()

for category, (_, company, funding) in result:
    print(f"{category}: Top Funded Company - {company} (Funding: ${funding:.2f}M)")

sc.stop()


