# README

## Rethinking Health Care Systems: What are the effects of governmental/social or private health insurance on total health expenditure?

Carla Garcia Medina and Mathilde Simoni <br/>
Professor Ann Malavet <br/>
Processing Big Data for Analytics Applications <br/>
Fall 2021 <br/>


### Project Description

This application aims to find the correlation between the type of health care system of a country and the country’s total health expenditure. 

### Files
The format used for this directory is different to that specified in the assignment instruction, since in order to test how the data cleaning affected the data, data cleaning and profiling steps were intercalated. The directory organization format shown below was used since it was a clear and simpler way of organizing the files and code.
* cleaning_profiling_dataset1/ : *directory with files to clean and profile dataset 1*
   * initial_mapreduce_cleaning/ : *direcotry with files for initial cleaning and profiling in the form of a java MapReduce job*
      * Clean.class : *compiled main function for the java cleaning MapReduce job*
      * Clean.java : *main function for the java cleaning MapReduce job*
      * CleanMapper.class : *compiled mapper function for the java cleaning MapReduce job*
      * CleanMapper.java : *mapper function for the java cleaning MapReduce job*
      * CleanReducer.class : *compiled reducer function for the java cleaning MapReduce job*
      * CleanReducer.java : *reducer function for the java cleaning MapReduce job*
      * CountRecs.class : *compiled main function for the java profiling MapReduce job*
      * CountRecs.java : *main function for the java profiling MapReduce job, which counts the number of records in a csv file*
      * CountRecsMapper.class : *compiled mapper function for the java profiling MapReduce job*
      * CountRecsMapper.java : *mapper function for the java profiling MapReduce job*
      * CountRecsReducer.class : *compiled reducer function for the java profiling MapReduce job*
      * CountRecsReducer.java : *reducer function for the java profiling MapReduce job*
      * GHED_data.csv : *copy of dataset1 containing information of healthcare expenses by country and year*
      * clean.jar : *jar file containing Clean, CleanMapper, and CleanReducer .java and .classes*
      * countRecs.jar : *jar file containing CountRecs, CountRecsMapper, and CountRecsReducer .java and .classes*
      * part-r-00000 : *output of cleaned dataset after the cleaning MapReduce job*
   * GHED_data.csv : *copy of dataset1 containing information of healthcare expenses by country and year* 
   * expenses_data_cleaning_and_profiling.py : *final pyspark script to clean and profile dataset1*
* cleaning_profiling_dataset2/ : *directory with files to clean and profile dataset 2*
  * initial_mapreduce_cleaning/ : *direcotry with files for initial cleaning and profiling in the form of a java MapReduce job*
      * Clean.class : *compiled main function for the java cleaning MapReduce job*
      * Clean.java : *main function for the java cleaning MapReduce job*
      * CleanMapper.class : *compiled mapper function for the java cleaning MapReduce job*
      * CleanMapper.java : *mapper function for the java cleaning MapReduce job*
      * CleanReducer.class : *compiled reducer function for the java cleaning MapReduce job*
      * CleanReducer.java : *reducer function for the java cleaning MapReduce job*
      * clean.jar : *jar file containing Clean, CleanMapper, and CleanReducer .java and .classes*
      * dataset_cleaned1.csv : *output of cleaned dataset after the cleaning MapReduce job*
   * dataset_initial.csv : *copy of dataset2 containing information about percentages of population for different types of healthcare coverage by country and year* 
   * coverage_data_cleaning_and_profiling.py : *final pyspark script to clean and profile dataset2*
* screenshots/
	* etl_cleaning_profiling_dataset1/ : *screenshots for dataset1*
		* mapreduce_cleaning_and_profiling/ : *mapreduce screenshots for dataset1*
		* pyspark_cleaning_and_profiling/: *pyspark screenshots for dataset1*
	* etl_cleaning_profiling_dataset2/ : *screenshots for dataset2*
		* initial_mapreduce_cleaning/ : *mapreduce screenshots for dataset2*
		* pyspark_cleaning_and_profiling/: *pyspark screenshots for dataset2*
	* final_analytic/ : *screenshots for merged dataset*
    * data_ingestion/ : *screenshots for data ingestion*
* analytics/
    * joined_datasets_analytics.py : *final pyspark script for merging, statistics and correlation between the 2 datasets*
* data_ingestion/
    * data_ingestion : *file containing data ingestion commands*
* README.md : *this file*

### Analytics steps

#### Data Ingestion
* The 2 initial datasets *GHED_data.csv* and *dataset_initial.csv* were added on Peel with the scp command
* Then, then they were added in the big-data-project directory in HDFS
* Finally access was shared with graders and teammates

#### Cleaning and Profiling for dataset1 (health expenditure)
* An initial profiling cleaning was done with two MapReduce jobs (contained in the initial_mapreduce_cleaning directory). 
  1. Initial Profiling <br/>
  The CountRecs job was first used to count the number of records in the original dataset1. The number of rows output was 3648. 
  2. Initial Cleaning <br/>
  The Clean job was then used to clean the data. All columns except for country and che_pc_usd were removed. Additionally, records with missing values in the che_pc_usd field were dropped. Only the values with information for the year 2018 were kept since this was the initial year of interest for the analytic.
  3. Final Profiling <br/>
  Running the CountRecs on the cleaned dataset (output in file with name part-r-00000), the number of rows output was 188. The new number of rows did not match the original one. The main reasons for this difference is that only rows with a year value of “2018” were selected since this was the initial year of interest for the analysis. Given that the data contained years from 2000 to 2018 inclusive, this resulted in a drastic reduction in the number of rows. Additionally, a few records had missing values for the che_pc_usd column. These records were removed since they were sparse and the countries in the dataset are extremely different to one another, such that imputing values could result in bias.<br/>
*Screenshots for this step can be found in the directory screenshots/etl_cleaning_profiling_dataset1/mapreduce_cleaning_and_profiling/*
  
* Deeper Cleaning and Profiling with Pyspark <br/>
It was later decided to use PySpark to perform a deeper cleaning and profiling of dataset 1. (no use of the mapreduce job in the final product)
  1. Profiling 
    * The original dataset is used to calculate the covariance between Current Health Expenditure per Capita in US and subtypes of this expenditure including domestic general, domestic private, out-of-pocket, and primary health care.
    * Descriptive statistics of these columns was obtained including the counts, means, standard deviations, minimum values, and maximum values.
    * A list of the unique values for the countries column was output to see the individual countries present in the dataset.
    * A list of the unique types of income groups present in the dataset was also shown.
    * The covariance between each expense subtype and Current Health Expenditure was calculated.
    
  2. Cleaning
    * Column names were renamed to improve readability and usability of the dataset (e.g. "income group (2018)" was renamed to "income_group").
    * The data types of the six main helath expenditure columns were casted to integer to be able to perform mathematical operations on these.
    * Only the "country", "income_group", "year", "che_pc_usd" columns were selected for the cleaned version of dataset1 since these would be the columns of interest for merging and the analytic.
    * The dataframe was filtered to only keep data for the last five years present in the dataset (2018 to 2014, inclusive).
    * All remaining records with NULL values were shown and since these formed a very small minority of the total records in the dataset, they were dropped.
    * The name of the "country" column was changed to "countries" so that it would not coincide with the name of the "countries" column in dataset2.
    * The names of some countries were renamed to match the names of countries in dataset2 so that merging the two datasets would be done correctly.
    * The final dataset was output. <br/>
*Screenshots for this step can be found in the directory screenshots/etl_cleaning_profiling_dataset1/pyspark_cleaning_and_profiling/*

#### Cleaning and Profiling for dataset2 (coverage)
* The first part of the cleaning was done using MapReduce (contained in the initial_mapreduce_cleaning directory). <br/>
The Clean job was used to clean the data. The file was first filtered to keep the data for the years 2014 to 2018 only. Then, the file was reorganized to contain 8 columns: "country", "year", "Total health care", "Total private", "Primary private", "Duplicate private", "Complementary private", "Supplementary private". The last 6 columns collect percentages of total population for different types of healthcare coverage for 39 country between 2014 and 2018. Finally, a missing value was replaced with the string "NONE". (The output was moved to the directory big-data-project in HDFS as dataset_cleaned1.csv if the graders want to proceed to the pyspark cleaning without doing the mapreduce job.) <br/>
*Screenshots for this step can be found in the directory screenshots/etl_cleaning_profiling_dataset2/initial_mapreduce_cleaning/*

* Pyspark was used for the second part of cleaning and for profiling <br/>
It was later decided to use PySpark to perform profiling and a deeper cleaning of dataset 2.
  1. Profiling 
	* Descriptive statistics was obtained including the counts, means, standard deviations, minimum values, and maximum values for the following columns and a particular year (2018 by default, can be changed)
		* "governmental_coverage"
		* "total_private_coverage"
		* "primary_private_coverage"
		* "duplicate_private_coverage"
		* "complementary_private_coverage"
		* "supplementary_private_coverage"
    * For each column, countries with the minimum, maximum and mean values were displayed.

  2. Cleaning
   * Column names were renamed to improve readability and usability of the dataset (e.g. "_c3" was renamed to "total_private_coverage").
   * The data types of the six main coverage columns were casted to float to be able to perform mathematical operations on these ("NONE" values were converted to NULL values).
    * The "\t" character was removed from all the values in the "year" column in order to cast the data type of this column to integer.
    * Only the "country", "year", "governmental_coverage" columns were selected for the cleaned version of dataset2 since these would be the columns of interest for merging and the analytic (The other interesting column "primary_private_coverage" was studied but contained too many NULL values: 79 for all countries and all years. Hence, it was also dropped). 
    * All remaining records with NULL values were shown. 
		* South Africa and Brazil had NULL values for the 5 years so they were dropped from the dataset.
        * Greece, Latvia and Luxembourg had NULL values for some years. Those specific records were dropped (not all the years).
    * The final dataset was output. <br/>
*Screenshots for this step can be found in the directory screenshots/etl_cleaning_profiling_dataset2/pyspark_cleaning_and_profiling/*

#### Merging and First correlation
* It was decided to merge the 2 dataframes `governmental_coverage_df` and `expenses_df` and calculate a correlation for each year between 2014 and 2018.
   * First, the 2 dataframes were filtered to only keep data for the year being analyzed.
   * Then, they were merged in a new dataframe `df` using the pyspark inner join function based on the country name.
   * Finally, the corellation was calculated using the pyspark corr function.
   * The describe function was used to obtain statistics about the resulting dataset. <br/>
*Screenshots for this step can be found in the directory screenshots/final_analytic/*

### Instructions to Run Code

1. Place the *clean.jar* file on Peel (you can find it in the directory *cleaning_profiling_dataset2/initial_mapreduce_cleaning/clean.jar* ): <br/>
`scp <path to *clean.jar* on local computer> <path to clean.jar on Peel>`
2. Log-in to Peel and run the MapReduce job in *cleaning_profiling_dataset2/initial_mapreduce_cleaning/* to make an initial clean of dataset2. The directory with the input file in HDFS has been shared with the graders. You must choose a directory to output the result. <br/>
`yarn jar`
`hadoop jar clean.jar Clean </user/mps565/big-data-project/dataset_initial.csv <path to result from step 2>`
3. Connect to python Spark interactive Shell: <br/>
`module load python/gcc/3.7.9`  <br/>
`pyspark --deploy-mode client`
4. Copy and paste commands from `cleaning_profiling_dataset1/expenses_data_cleaning_and_profiling.py` to clean and profile dataset1 into the interactive Shell.
5. In the python file `cleaning_profiling_dataset2/coverage_data_cleaning_and_profiling.py`, edit the variable path to correspond to the path where you stored the result of the initial mapreduce job (from step 2) in hdfs: <br/> 
 For example:<br/>
 `path = "/user/mps565/big-data-project/dataset_cleaned1.csv"`<br/>
 `# path = ` <br/>
 *For convenience, the result of the mapreduce job has also been added to the shared directory. If you did not run the mapreduce job, do not change the variable path.* 
6. Copy and paste commands from `cleaning_profiling_dataset2/coverage_data_cleaning_and_profiling.py` to clean and profile dataset2 into the interactive Shell.
7. Copy and paste commands from `joined_datasets_analytics` to merge the two datasets and compute the merged analytics.

### Troubleshooting

If permission errors appear when opening the files, follow the instructions below:
1. Place the *clean.jar* file and the 2 initial datasets *GHED_data.csv* and *dataset_initial.csv* on Peel <br/>
	* *clean.jar* can be found in the directory cleaning_profiling_dataset2/initial_mapreduce_cleaning/
	* *GHED_data.csv* can be found in the directory cleaning_profiling_dataset1/GHED_data.csv
	* *dataset_initial.csv* can be found in the directory cleaning_profiling_dataset2/dataset_initial.csv 
`scp <path to *clean.jar* on local computer> <path to *clean.jar* on Peel>`
`scp <path to *GHED_data.csv* on local computer> <path to *GHED_data.csv* on Peel>`
`scp <path to *dataset_initial.csv* on local computer> <path to *dataset_initial.csv* on Peel>`
2. Place the input files with dataset1 and dataset2 (`cleaning_profiling_dataset1/GHED_data.csv` and ` cleaning_profiling_dataset2/dataset_initial.csv`) onto hdfs:  <br/>
`hdfs dfs -put <path to dataset1 in peel> <path to dataset1 in hdfs>` <br/>
`hdfs dfs -put <path to dataset2 in peel> <path to dataset2 in hdfs>`
3. Compile and run the MapReduce job in `cleaning_profiling_dataset2/initial_mapreduce_cleaning/` to make an initial clean of dataset2.<br/>
`hadoop jar clean.jar Clean <path to dataset2 in hdfs> <path to output>`
3. Connect to python Spark interactive Shell: <br/>
`module load python/gcc/3.7.9` <br/>
`pyspark --deploy-mode client`
5. In the python file `cleaning_profiling_dataset1/expenses_data_cleaning_and_profiling.py`, edit the variable path to correspond to the path where you stored dataset1 in hdfs: <br/> 
For example:<br/>
`# create path to input file` <br/>
`#path = "project/code_drop1/GHED_data.csv"` <br/>
`path = "/user/cgm396/hw8/GHED_data.csv"` <br/>
5. Copy and paste commands from `cleaning_profiling_dataset1/expenses_data_cleaning_and_profiling.py` to clean and profile dataset1 into the interactive Shell.
6. In the python file `cleaning_profiling_dataset2/coverage_data_cleaning_and_profiling.py`, edit the variable path to correspond to the path where you stored the result of the initial mapreduce job (from step 2) in hdfs: <br/> 
For example:<br/>
`# create path to input file` <br/>
`#path = "project/code_drop1/result.csv"` <br/>
`path = "/user/cgm396/hw8/result.csv"` <br/>
8. Copy and paste commands from `cleaning_profiling_dataset2/coverage_data_cleaning_and_profiling.py` to clean and profile datset2 into the interactive Shell.
9. Copy and paste commands from `joined_datasets_analytics` to merge the two datasets and compute the merged analytics.

A *UnicodeEncodeError* may appear when copy-pasting the python scripts in pyspark. In that case, try running the following commands: <br/>
`import sys` <br/>
`import codecs` <br/>
`sys.stdout = codecs.getwriter('utf8')(sys.stdout)` <br/>

