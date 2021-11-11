from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
from pyspark.sql.functions import *

# 1st corelation for year 2018
governmental_coverage_2018_df = governmental_coverage_df.filter((governmental_coverage_df.year == 2018)
governmental_coverage_2018_df.show()
governmental_coverage_2018_df.count()

expenses_2018_df = expenses_df.filter((expenses_df.year == "2018"))
expenses_2018_df.show(10)
expenses_2018_df.count()

df = governmental_coverage_2018_df.join(expenses_2018_df, expenses_2018_df.countries == governmental_coverage_2018_df.country, "inner")
df = df.drop("countries")
df = df.drop("year")
df.corr("governmental_coverage","che_pc_usd")  