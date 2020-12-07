/*
	Script to create textfile with person names + identifiers
	Run command: pig -x local /usr/local/pig_data/person_extraction.pig
	Run command: pig -x mapreduce hdfs://localhost:9000/pig_extraction/person_extraction.pig
*/

-- register my UDF in python
-- register '/usr/local/pig_data/my_functions.py' using jython as myfuncs;
register 'hdfs://localhost:9000/python_functions/my_functions.py' using jython as myfuncs;

-- local mode load function
data = LOAD 'hdfs://localhost:9000/freebase_data/freebase.gz' USING PigStorage('\t') AS (subject:chararray, predicate:chararray, object:chararray);
data = LIMIT data 1000000;
-- generate just object name data
name_data = FILTER data BY (predicate == '<http://rdf.freebase.com/ns/type.object.name>');
name_data = DISTINCT(FOREACH name_data GENERATE subject, object);
-- generate just identifiers of persons
identifiers = FILTER data BY (object == '<http://rdf.freebase.com/ns/people.person>') AND (predicate == '<http://rdf.freebase.com/ns/type.object.type>');
identifiers = DISTINCT(FOREACH identifiers GENERATE subject AS unique_subject);
-- join just identifiers of persons and object name data and store output
joined_data = JOIN identifiers BY unique_subject, name_data BY subject;
joined_data = DISTINCT(FOREACH joined_data GENERATE subject, object);
-- call python UDF to clear names
grouped_data = GROUP joined_data BY subject;
output_data = FOREACH grouped_data GENERATE myfuncs.clear_names(group, joined_data);
output_data = FOREACH output_data GENERATE tuple_0.subject, tuple_0.object;
STORE output_data INTO 'hdfs://localhost:9000/person_output/' USING PigStorage('\t');
