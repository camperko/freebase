# Freebase parsing
Repository for VI subject.

Read freebase compressed file and parse it using hadoop pig and python to find information persons.

# How to run
Download freebase data dump from: https://developers.google.com/freebase 

Rename file to freebase.gz

### Put freebase data dump to hdfs
hdfs dfs -mkdir hdfs://localhost:9000/freebase_data

hdfs dfs -put /home/Download/freebase.gz dfs://localhost:9000/freebase_data/

### Put python udfs to hdfs
hdfs dfs -mkdir hdfs://localhost:9000/python_functions

hdfs dfs -put /home/Download/my_functions.py dfs://localhost:9000/python_functions/

### Put script for people extraction to hdfs
hdfs dfs -mkdir hdfs://localhost:9000/pig_extraction

hdfs dfs -put /home/Download/person_extraction.pig dfs://localhost:9000/pig_extraction/

### Put script to extract all data of persons to hdfs
hdfs dfs -mkdir hdfs://localhost:9000/pig_data

hdfs dfs -put /home/Download/create_data.pig dfs://localhost:9000/pig_data/

### Run scripts
pig -x mapreduce hdfs://localhost:9000/pig_extraction/person_extraction.pig

pig -x mapreduce hdfs://localhost:9000/pig_data/create_data.pig

# Output
Schema of person output data:
 - name: string | array[string]
 - gender: string | array[string]
 - name_aliases: string | array[string]
 - nationality: string | array[string]
 - profession: string | array[string]
 - birth_date: string | array[string]
 - birth_place: string | array[string]
 - death_date: string | array[string]
 - death_place: string | array[string]
 - burial_place: string | array[string]
 - type_information: string | array[string]
 - description: string | array[string]
 - weight: float | array[float]
 - height: float | array[float]
 - webpages: string | array[string]
