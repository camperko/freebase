#!/usr/bin/python

# Run command: pig -x local /usr/local/pig_data/python_exe.py "'Katrin Marras'"

# rozbalovat, pokym bude existovat identifikator
# vyhodnocovanie -> priemer osoby vlastnosti / najviac / najmenej

from org.apache.pig.scripting import *
import sys

def is_final(final_iterator):
	for pig_tuple in final_iterator:
		print pig_tuple
		p_object = str(pig_tuple.get(2))
		if p_object[1] == '.':
			return False
	return True

def find_object_identifiers(matches_iterator):
	identifiers = []
	for pig_tuple in matches_iterator:
		p_object = str(pig_tuple.get(2))
		if p_object[1] == '.':
			identifiers.append(p_object)
	return identifiers

if __name__ == '__main__':
	P = Pig.compile("""
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
	-- clear data to readable format
	readable_data = FOREACH unique_person_data GENERATE myfuncs.clear_data(filtered_data::subject, filtered_data::predicate, filtered_data::object);
	readable_data = FOREACH readable_data GENERATE tuple_0.subject, tuple_0.predicate, tuple_0.p_object;
	DUMP readable_data;
	""")
	params = {'name': sys.argv[1]}
	bound = P.bind(params)
	stats = bound.runSingle()
	if not stats.isSuccessful():
		raise 'failed'
	print '-------------------------------------------------------------------------------------------------------------------------------------'
	f_iterator = stats.result("readable_data").iterator()
	m_iterator = stats.result("readable_data").iterator()
	while not is_final(f_iterator):
		identifiers = find_object_identifiers(m_iterator)
		print identifiers
		# pig join
		break
	