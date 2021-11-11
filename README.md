# README

## Rethinking Health Care Systems: What are the effects of governmental/social or private health insurance on total health expenditure?

Carla Garcia Medina and Mathilde Simoni <br/>
Professor Ann Malavet <br/>
Processing Big Data for Analytics Applications <br/>
Fall 2021 <br/>


### Project Description

This application aims to find the correlation between the type of health care system of a country and the country’s total health expenditure. 

### Files
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
      * clean.jar : *jar file containing clean, cleanMapper, and cleanReducer .java and .classes*
      * countRecs.jar : *jar file containing countRecs, countRecsMapper, and countRecsReducer .java and .classes*
      * part-r-00000 : *output of cleaned dataset after the cleaning MapReduce job*
   * GHED_data.csv : *copy of dataset1 containing information of healthcare expenses by country and year* 
   * expenses_data_cleaning_and_profiling.py : *final pyspark script to clean and profile dataset1*
* cleaning_profiling_dataset2/ : *directory with files to clean and profile dataset 2*
  * **----------TO-DO------------**
* README.md : *this file*
* joined_datasets_analytics.py : **----------TO-DO------------**

### Analytics steps

#### Cleaning and Profiling for dataset1 (health expenditure)
* An initial profiling cleaning was done with two MapReduce jobs (contained in the initial_mapreduce_cleaning directory). 
  1. Initial Profiling <br/>
  The CountRecs job was first used to count the number of records in the original dataset1. The number of rows output was 3648. 
  ![alt text](https://github.com/carla-garcia-medina/big-data-project/blob/main/cleaning_profiling_dataset1/initial_mapreduce_cleaning/screenshots/pasted%20image%200.png)
  ![alt text](https://github.com/carla-garcia-medina/big-data-project/blob/main/cleaning_profiling_dataset1/initial_mapreduce_cleaning/screenshots/pasted%20image%200%20(1).png)
  ![alt text](https://github.com/carla-garcia-medina/big-data-project/blob/main/cleaning_profiling_dataset1/initial_mapreduce_cleaning/screenshots/pasted%20image%200%20(2).png)
  2. Initial Cleaning <br/>
  The Clean job was then used to clean the data. All columns except for country and che_pc_usd were removed. Additionally, records with missing values in the che_pc_usd field were dropped. Only the values with information for the year 2018 were kept since this was the initial year of interest for the analytic.
  ![alt text](https://github.com/carla-garcia-medina/big-data-project/blob/main/cleaning_profiling_dataset1/initial_mapreduce_cleaning/screenshots/pasted%20image%200%20(4).png)
  ![alt text](https://github.com/carla-garcia-medina/big-data-project/blob/main/cleaning_profiling_dataset1/initial_mapreduce_cleaning/screenshots/pasted%20image%200%20(5).png)
  ![alt text](https://github.com/carla-garcia-medina/big-data-project/blob/main/cleaning_profiling_dataset1/initial_mapreduce_cleaning/screenshots/pasted%20image%200%20(6).png)
  ![alt text](https://github.com/carla-garcia-medina/big-data-project/blob/main/cleaning_profiling_dataset1/initial_mapreduce_cleaning/screenshots/pasted%20image%200%20(7).png)
 3. Final Profiling <br/>
  Running the CountRecs on the cleaned dataset (output in file with name part-r-00000), the number of rows output was 188. The new number of rows did not match the original one. The main reasons for this difference is that only rows with a year value of “2018” were selected since this was the initial year of interest for the analysis. Given that the data contained years from 2000 to 2018 inclusive, this resulted in a drastic reduction in the number of rows. Additionally, a few records had missing values for the che_pc_usd column. These records were removed since they were sparse and the countries in the dataset are extremely different to one another, such that imputing values could result in bias.
  ![alt text](https://github.com/carla-garcia-medina/big-data-project/blob/main/cleaning_profiling_dataset1/initial_mapreduce_cleaning/screenshots/pasted%20image%200%20(8).png)
  ![alt text](https://github.com/carla-garcia-medina/big-data-project/blob/main/cleaning_profiling_dataset1/initial_mapreduce_cleaning/screenshots/pasted%20image%200%20(9).png)
  
* Deeper Cleaning and Profiling with Pyspark <br/>
It was later decided to use PySpark to perform a deeper cleaning and profiling of dataset 1.
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
    * The final dataset was output.

    **----------ADD SCREENSHOTS------------**

#### Cleaning and Profiling for dataset2 (coverage)
* The first part of the cleaning was done using MapReduce **----------TO-DO------------**
* Pyspark was used for the second part of cleaning and for profiling **----------TO-DO------------**

#### Merging and First correlation
* **----------TO-DO------------**
* **----------TO-DO------------**

### Instructions to Run Code
