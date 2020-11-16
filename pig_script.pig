/*
	Call script like to pass name of person:
	pig -x local -param name="'Katrin Marras'" /usr/local/pig_data/pig_script.pig
*/

-- new version of script
register '/usr/local/pig_data/my_functions.py' using jython as myfuncs;
-- load all data and identifiers of persons
all_data = LOAD '/usr/local/pig_data/freebase.gz' USING PigStorage('\t') AS (subject:chararray, predicate:chararray, object:chararray);
all_data = LIMIT all_data 2500000;
identifier_data = LOAD '/usr/local/pig_data/persons.txt' USING PigStorage('\t') AS (subject:chararray, object:chararray);
-- find identifier of selected person
identifier = DISTINCT(FILTER identifier_data BY (object == '$name'));
identifier = LIMIT identifier 1;
-- filter all data to include freebase name_space and exclude base data
filtered_data = FILTER all_data BY (predicate MATCHES '.*http://rdf.freebase.com.*') AND NOT(object MATCHES '.*http://rdf.freebase.com/ns/base.*');
-- join filtered data and identifier
unique_person_data = JOIN identifier BY subject, filtered_data BY subject;
unique_person_data = DISTINCT(FOREACH unique_person_data GENERATE filtered_data::subject, filtered_data::predicate, filtered_data::object);
DUMP unique_person_data;
-- clear data to readable format
readable_data = FOREACH unique_person_data GENERATE myfuncs.clear_data(filtered_data::subject, filtered_data::predicate, filtered_data::object);
readable_data = FOREACH readable_data GENERATE tuple_0.subject, tuple_0.predicate, tuple_0.p_object;
DUMP readable_data;