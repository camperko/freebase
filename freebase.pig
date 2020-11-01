/*
	Call script like to pass name of person:
	pig -x local -param name='Katrin Marras' /usr/local/pig_data/pig_script.pig
	rozbalovat, pokym bude existovat identifikator
*/
-- 
-- register my UDF in python
register '/usr/local/pig_data/my_functions.py' using jython as myfuncs;

/* 
	We have 2 modes mapreduce and local
	Mapreduce mode is working with hdfs so data load function looks like this
	data = LOAD '/freebase.gz' USING PigStorage('\t') AS (subject:chararray, predicate:chararray, object:chararray, dot:chararray);
 */ 

-- local mode load function
data = LOAD '/usr/local/pig_data/freebase.gz' USING PigStorage('\t') AS (subject:chararray, predicate:chararray, object:chararray);
-- limit data to 1000000 samples
top_data = LIMIT data 10000000;
-- select just data, where object is of type person and predicate is define as type; also delete all data with object with 'base'; also we want data just fwithrom freebase predicate
filter_data = FILTER top_data BY (predicate MATCHES '.*http://rdf.freebase.com.*') AND NOT(object MATCHES '.*http://rdf.freebase.com/ns/base.*');
person_data = FILTER filter_data BY (object == '<http://rdf.freebase.com/ns/people.person>') AND (predicate == '<http://rdf.freebase.com/ns/type.object.type>');
-- generate just identifiers of persons
identifiers = DISTINCT(FOREACH person_data GENERATE subject AS unique_subject);
-- join identifiers with limited data so we have all data for person type and delete reduntant subject at start
joined_data = JOIN identifiers BY unique_subject, filter_data BY subject;
cleared_joined_data = DISTINCT(FOREACH joined_data GENERATE myfuncs.clear_object(subject, predicate, object));
cleared_joined_data = FOREACH cleared_joined_data GENERATE tuple_0.subject, tuple_0.predicate, tuple_0.p_object;
-- find information only about given person
-- 	name_data = FILTER cleared_joined_data BY (predicate MATCHES '.*type.object.name.*') AND (p_object == 'Katrin Marras');
name_data = FILTER cleared_joined_data BY (predicate MATCHES '.*type.object.name.*') AND (p_object == '$name');
unique_identifier = DISTINCT(FOREACH name_data GENERATE subject);
unique_person_data = JOIN unique_identifier BY subject, cleared_joined_data BY subject;
unique_person_data = FOREACH unique_person_data GENERATE unique_identifier.subject, predicate, p_object;

-- call UDF to transform data to more readable format
readable_data = FOREACH unique_person_data GENERATE myfuncs.clear_data(unique_identifier.subject, predicate, p_object);
readable_data = FOREACH readable_data GENERATE tuple_0.subject, tuple_0.predicate, tuple_0.p_object;
-- group data by subject, so we have all data for each unique subject

-- grouped_readable_data = GROUP readable_data BY subject;
