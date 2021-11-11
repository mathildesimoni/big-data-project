# README

## Rethinking Health Care Systems: What are the effects of governmental/ social or private health insurance on total health expenditure?
Carla Garcia Medina and Mathilde Simoni
Professor Ann Malavet
Processing Big Data for Analytics Applications
Fall 2021


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
* cleaning_profiling_dataset2/ : *directory with files to clean and profile dataset 1*
  * **----------TO-DO------------**
* README.md : *this file*
* joined_datasets_analytics.py : **----------TO-DO------------**

### Analytics steps

#### Cleaning and Profiling for dataset1 (health expenditure)
* Pyspark **----------TO-DO------------**

#### Cleaning and Profiling for dataset2 (coverage) ####
* The first part of the cleaning was done using MapReduce **----------TO-DO------------**
* Pyspark was used for the second part of cleaning and for profiling **----------TO-DO------------**

#### Merging and First correlation ####
* **----------TO-DO------------**
* **----------TO-DO------------**
