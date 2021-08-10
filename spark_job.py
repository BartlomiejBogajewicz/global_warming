import findspark
findspark.init()

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from operator import add


config = SparkConf().setAppName('temp_transformation').setMaster('local[*]')

sc = SparkContext.getOrCreate(conf=config)

spark_sql = SparkSession.builder.appName("read csv").getOrCreate()

#read csv
spark_csv = spark_sql.read.csv('city_temperature.csv',header = True)

def farenheit_to_cel(temp : "temp_in_faren") -> "temp_in_cel":
    return (float(temp) -32) * 0.56 

rdd = spark_csv.rdd

#mean temp grouped by country, year
#rows with temp = -99 are treated as errors in dataset so I filter them out

rdd_mapped_temp = rdd.filter(lambda x: x[7] != '-99').map(lambda x: [' '.join([x[1],x[6]]),[farenheit_to_cel(x[7]),1]])

rdd_agg_temp = rdd_sum_temp.reduceByKey(lambda a,b: [a[0]+b[0],a[1]+b[1]]).mapValues(lambda x: round(x[0]/x[1],2))

rdd_result = rdd_agg_temp.sortByKey().collect()





