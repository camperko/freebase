-- register my own UDF in python
register '/usr/local/pig_data/my_functions.py' using jython as myfuncs;

/* 
	We have 2 modes mapreduce and local
	Mapreduce mode is working with hdfs so data load function looks like this
	data = LOAD '/freebase.gz' USING PigStorage('\t') AS (subject:chararray, predicate:chararray, object:chararray, dot:chararray);
 */ 

-- local mode load function
data = LOAD '/usr/local/pig_data/freebase.gz' USING PigStorage('\t') AS (subject:chararray, predicate:chararray, object:chararray, dot:chararray);
-- delete dot at the end of loaded data and limit data to 500000 samples
clean_data = FOREACH data generate subject, predicate, object;
top_clean_data = LIMIT clean_data 500000;
-- select just data, where object is of type person and predicate is define as type
filter_data = FILTER top_clean_data BY (object == '<http://rdf.freebase.com/ns/people.person>') AND (predicate == '<http://rdf.freebase.com/ns/type.object.type>');
-- generate just identifiers of persons
identifiers = FOREACH filter_data GENERATE subject AS unique_subject;
-- join identifiers with limited data so we have all data for person type and delete reduntant subject at start
joined_data = JOIN identifiers BY unique_subject, top_clean_data BY subject;
cleared_joined_data = FOREACH joined_data GENERATE subject, predicate, object;
-- we want data just from freebase rdf
filtered_joined_data = FILTER cleared_joined_data BY (predicate MATCHES '.*http://rdf.freebase.com.*');
-- call UDF to transform data to more readable format
readable_data = FOREACH filtered_joined_data GENERATE myfuncs.clear_data(subject, predicate, object);
readable_data = FOREACH readable_data GENERATE tuple_0.subject, tuple_0.predicate, tuple_0.p_object;
-- group data by subject, so we have all data for each unique subject
grouped_readable_data = GROUP readable_data BY subject;