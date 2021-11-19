# import necessary libraries
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# create path to input file
#path = "project/code_drop1/GHED_data.csv"
path = "/user/cgm396/hw8/GHED_data.csv"

# convert data source to spark dataframe
expenses_df = sqlContext.read.option("header", True).csv(path)

# rename column for ease of use
expenses_df = expenses_df.withColumnRenamed("income group (2018)", "income_group")

expenses_df = expenses_df.withColumn("che_pc_usd", expenses_df.che_pc_usd.cast(IntegerType()))
expenses_df = expenses_df.withColumn("gghed_pc_usd", expenses_df.gghed_pc_usd.cast(IntegerType()))
expenses_df = expenses_df.withColumn("pvtd_pc_usd", expenses_df.pvtd_pc_usd.cast(IntegerType()))
expenses_df = expenses_df.withColumn("oop_pc_usd", expenses_df.oop_pc_usd.cast(IntegerType()))
expenses_df = expenses_df.withColumn("phc_usd_pc", expenses_df.phc_usd_pc.cast(IntegerType()))

#---GENERAL DATA PROFILING---
# Current Health Expenditure (CHE) per Capita in US$
# Domestic General Government Health Expenditure (GGHE-D) per Capita in US$
# Domestic Private Health Expenditure (PVT-D) per Capita in US$
# Out-of-Pocket Expenditure (OOPS) per Capita in US$
# Primary Health Care (PHC) Expenditure per Capita in US$
expenses_df.describe(['che_pc_usd','gghed_pc_usd','pvtd_pc_usd','oop_pc_usd','phc_usd_pc']).show()

expenses_df.withColumn('gghed_over_che', col('gghed_pc_usd') / col('che_pc_usd'))
expenses_df.withColumn('pvtd_over_che', col('pvtd_pc_usd') / col('che_pc_usd'))
expenses_df.withColumn('oop_over_che', col('oop_pc_usd') / col('che_pc_usd'))
expenses_df.withColumn('phc_over_che', col('phc_usd_pc') / col('che_pc_usd'))

expenses_df.describe(['gghed_over_che','pvtd_over_che','oop_over_che','phc_over_che']).show()

# Show income groups in dataset
expenses_df.select('country').distinct().sort('country').show(200)
income_group_df = expenses_df.select('income_group').distinct()
income_group_df.show()
income_group_collect = income_group_df.collect()
for year in range(2015, 2016): # parameters can be changed depending on how many years want to be analyzed
	for group in income_group_collect:
		print(year, str(group['income_group']))
		expenses_by_income_group_df = expenses_df.filter("income_group = '{0}' AND year = {1}".format(str(group['income_group']), year))
		expenses_by_income_group_df.describe(['che_pc_usd']).show()
		# Calculate covariance between che_pc_usd and other columns
		expenses_by_income_group_df.cov('che_pc_usd','gghed_pc_usd')
		expenses_by_income_group_df.cov('che_pc_usd','pvtd_pc_usd')
		expenses_by_income_group_df.cov('che_pc_usd','oop_pc_usd')
		expenses_by_income_group_df.cov('che_pc_usd','phc_usd_pc')
		print("")

#---PREPARE DATAFRAME FOR MERGING WITH SECOND DATA SOURCE---
# keep only the columns that will be used for merging
expenses_df = expenses_df.select("country", "income_group", "year", "che_pc_usd")
# keep only records with data for 2018
expenses_df = expenses_df.filter('year = 2018 OR year = 2017 OR year = 2016 OR year = 2015 OR year = 2014')
# show records with NULL values
expenses_df.filter("che_pc_usd is NULL OR 'income_group (2018)' is NULL OR Country is NULL").select("country").distinct().show()
# since only three countries have null values, these have been decided to be dropped
expenses_df.na.drop(subset=["country", "income_group", "year", "che_pc_usd"]).show()
# drop year column since values would all be 2018
#expenses_df = expenses_df.drop("year")
# rename country column to countries in order to properly merge dataframe with the OECD health coverage types data source
expenses_df = expenses_df.withColumnRenamed("country", "countries")
# rename country names to match those in the OECD data source
expenses_df = expenses_df.withColumn('countries', regexp_replace('countries', 'United States of America', 'United States'))
expenses_df = expenses_df.withColumn('countries', regexp_replace('countries', 'Republic of Korea', 'Korea'))
expenses_df = expenses_df.withColumn('countries', regexp_replace('countries', 'Slovakia', 'Slovak Republic'))
expenses_df = expenses_df.withColumn('countries', regexp_replace('countries', 'Russian Federation', 'Russia'))
expenses_df.sort('year').show(200)






