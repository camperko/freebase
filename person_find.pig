/*
	Script to list all persons with matching name
	Run command: pig -x local -param name="'Rob'" /usr/local/pig_data/person_find.pig
*/

data = LOAD '/usr/local/pig_data/persons.txt' USING PigStorage('\t') AS (subject:chararray, object:chararray);
output_data = FILTER data BY (object MATCHES '.*$name.*');
output_data = FOREACH output_data GENERATE object;
DUMP output_data;