# import libraries
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# define directory path
path = "project/code_drop1/dataset_cleaned1.csv"
# path = "hw8/dataset_cleaned1.csv"

# convert data source to spark dataframe
coverage_df = sqlContext.read.csv(path) 

# rename column for ease of use
coverage_df = coverage_df.withColumnRenamed("_c0", "country")\
.withColumnRenamed("_c1", "year")\
.withColumnRenamed("_c2", "governmental_coverage")\
.withColumnRenamed("_c3", "total_private_coverage")\
.withColumnRenamed("_c4", "primary_private_coverage")\
.withColumnRenamed("_c5", "duplicate_private_coverage")\
.withColumnRenamed("_c6", "complementary_private_coverage")\
.withColumnRenamed("_c7", "supplementary_private_coverage")

# change column types
coverage_df = coverage_df.withColumn("governmental_coverage", coverage_df.governmental_coverage.cast(FloatType()))
coverage_df = coverage_df.withColumn("total_private_coverage", coverage_df.total_private_coverage.cast(FloatType()))
coverage_df = coverage_df.withColumn("primary_private_coverage", coverage_df.primary_private_coverage.cast(FloatType()))
coverage_df = coverage_df.withColumn("duplicate_private_coverage", coverage_df.duplicate_private_coverage.cast(FloatType()))
coverage_df = coverage_df.withColumn("complementary_private_coverage", coverage_df.complementary_private_coverage.cast(FloatType()))
coverage_df = coverage_df.withColumn("supplementary_private_coverage", coverage_df.supplementary_private_coverage.cast(FloatType()))

coverage_df = coverage_df.withColumn("year", regexp_replace("year", "2014\t", "2014"))
coverage_df = coverage_df.withColumn("year", regexp_replace("year", "2015\t", "2015"))
coverage_df = coverage_df.withColumn("year", regexp_replace("year", "2016\t", "2016"))
coverage_df = coverage_df.withColumn("year", regexp_replace("year", "2017\t", "2017"))
coverage_df = coverage_df.withColumn("year", regexp_replace("year", "2018\t", "2018"))
coverage_df = coverage_df.withColumn("year", coverage_df.year.cast(IntegerType()))

coverage_df.printSchema()
coverage_df.show()

#---GENERAL DATA PROFILING---

year = 2018 # parameter can be changed depending on which year we analyze

coverage_df_by_year = coverage_df.where(col("year") == year)

columns = ["governmental_coverage", \
    "total_private_coverage", \
    "primary_private_coverage", \
    "duplicate_private_coverage", \
    "complementary_private_coverage", \
    "supplementary_private_coverage"]
profiling_df = coverage_df_by_year.describe(columns)
profiling_df.show()
profiling_df.printSchema()

# print statistics for every column variable (except year and country)
for column in columns:
    print("---------")
    print("VARIABLE: " + column)
    print("---------")
    
    # mean value
    mean_val = profiling_df.where(col("summary") == "mean").select(column).collect()[0][0]
    print("- mean value: " + mean_val)
    print("")
    
    # minimum value
    min_val = profiling_df.where(col("summary") == "min").select(column).collect()[0][0]
    countries_min_val = coverage_df_by_year.where(col(column) == min_val).select("country")
    print("- minimum value: " + min_val)
    print("countries with the minimun value: ")
    countries_min_val.show(200)
    
    # maximum value
    max_val = profiling_df.where(col("summary") == "max").select(column).collect()[0][0]
    countries_max_val = coverage_df_by_year.where(col(column) == max_val).select("country")
    print("- maximum value: " + max_val)
    print("countries with the maximum value: ")
    countries_max_val.show(200)
    
    print("\n")

#---PREPARE DATAFRAME FOR MERGING WITH SECOND DATA SOURCE ---

# dataframe 1: governmental_coverage_df
# keep only the columns that will be used for merging
governmental_coverage_df = coverage_df.select("country", "year", "governmental_coverage")
# show records with NULL values
governmental_coverage_df.filter(governmental_coverage_df.governmental_coverage.isNull()).show()
# remove south Africa and Brazil as they have null values for the 5 years
governmental_coverage_df = governmental_coverage_df.filter((governmental_coverage_df.country != "Brazil") & (governmental_coverage_df.country != "South Africa"))
# remove Greece, Latvia and luxembourg for the years they have no value
governmental_coverage_df = governmental_coverage_df.filter(governmental_coverage_df.governmental_coverage.isNotNull())
governmental_coverage_df.show()

# dataframe 2: private_coverage_df
# keep only the columns that will be used for merging
private_coverage_df = coverage_df.select("country", "year", "primary_private_coverage")
# show records with NULL values
private_coverage_df.filter(private_coverage_df.primary_private_coverage.isNull()).show()
private_coverage_df.filter(private_coverage_df.primary_private_coverage.isNull()).count()
# too many NULL values (79). We can't use this variable


