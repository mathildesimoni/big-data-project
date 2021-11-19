from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
from pyspark.sql.functions import *

for year in range(2014, 2019):
  print("Year: " + str(year))
  governmental_coverage_year_df = governmental_coverage_df.filter(governmental_coverage_df.year == year)
  governmental_coverage_year_df.show()
  governmental_coverage_year_df.count()
  print("\n")
  
  expenses_year_df = expenses_df.filter(expenses_df.year == str(year))
  expenses_year_df.show(10)
  expenses_year_df.count()
  print("")
  
  df = governmental_coverage_year_df.join(expenses_year_df, expenses_year_df.countries == governmental_coverage_year_df.country, "inner")
  df = df.drop("countries")
  df = df.drop("year")
  
  df.corr("governmental_coverage","che_pc_usd")
  
  print("----------")
  print("STATISTICS")
  print("----------")
  
  statistics_df = df.describe("governmental_coverage","che_pc_usd")
  statistics_df.show()
  
  min_val = statistics_df.where(col("summary") == "min").select("governmental_coverage").collect()[0][0]
  print("Minimum value for government insurance coverage: " + min_val)
  countries_min_val = df.where(col("governmental_coverage") == min_val).select("country", "income_group", "che_pc_usd")
  print("Countries with the minimum value for governmental insurance coverage and their total health expenditure: ")
  countries_min_val.show(200)
  
  max_val = statistics_df.where(col("summary") == "max").select("governmental_coverage").collect()[0][0]
  print("Maximum value for government insurance coverage: " + max_val)
  countries_max_val = df.where(col("governmental_coverage") == max_val).select("country", "income_group", "che_pc_usd")
  print("Countries with the maximum value for governmental insurance coverage and their total health expenditure: ")
  countries_max_val.show(200)
  
  print("")
