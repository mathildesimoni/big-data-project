from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
from pyspark.sql.functions import *

for year in range(2014, 2019):
  governmental_coverage_year_df = governmental_coverage_df.filter(governmental_coverage_df.year == year)
  governmental_coverage_year_df.show()
  governmental_coverage_year_df.count()
  
  expenses_year_df = expenses_df.filter(expenses_df.year == str(year))
  expenses_year_df.show(10)
  expenses_year_df.count()
  
  df = governmental_coverage_year_df.join(expenses_year_df, expenses_year_df.countries == governmental_coverage_year_df.country, "inner")
  df = df.drop("countries")
  df = df.drop("year")
  
  print("Year: " + str(year))
  df.corr("governmental_coverage","che_pc_usd")  
  df.describe("governmental_coverage","che_pc_usd").show()
  print("")
