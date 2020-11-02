#!/usr/bin/python

# Run command: pig -x local /usr/local/pig_data/python_exe.py "'Katrin Marras'"

# rozbalovat, pokym bude existovat identifikator
# vyhodnocovanie -> priemer osoby vlastnosti / najviac / najmenej

from org.apache.pig.scripting import *
import sys

def is_final(iterator):
	for pig_tuple in iterator:
		p_object = str(pig_tuple.get(2))
		if p_object[1] == '.':
			return False
	return True

def find_object_identifiers(iterator):
	identifiers = []
	for pig_tuple in iterator:
		p_object = str(pig_tuple.get(2))
		if p_object[1] == '.':
			identifiers.append(p_object)
	return identifiers

if __name__ == '__main__':
	P = Pig.compile("""
	register '/usr/local/pig_data/my_functions.py' using jython as myfuncs;
	data = LOAD '/usr/local/pig_data/freebase.gz' USING PigStorage('\t') AS (subject:chararray, predicate:chararray, object:chararray);
	top_data = LIMIT data 10000000;
	filter_data = FILTER top_data BY (predicate MATCHES '.*http://rdf.freebase.com.*') AND NOT(object MATCHES '.*http://rdf.freebase.com/ns/base.*');
	person_data = FILTER filter_data BY (object == '<http://rdf.freebase.com/ns/people.person>') AND (predicate == '<http://rdf.freebase.com/ns/type.object.type>');
	identifiers = DISTINCT(FOREACH person_data GENERATE subject AS unique_subject);
	joined_data = JOIN identifiers BY unique_subject, filter_data BY subject;
	cleared_joined_data = DISTINCT(FOREACH joined_data GENERATE myfuncs.clear_object(subject, predicate, object));
	cleared_joined_data = FOREACH cleared_joined_data GENERATE tuple_0.subject, tuple_0.predicate, tuple_0.p_object;
	name_data = FILTER cleared_joined_data BY (predicate MATCHES '.*type.object.name.*') AND (p_object == '$name');
	unique_identifier = DISTINCT(FOREACH name_data GENERATE subject);
	unique_person_data = JOIN unique_identifier BY subject, cleared_joined_data BY subject;
	unique_person_data = FOREACH unique_person_data GENERATE unique_identifier.subject, predicate, p_object;
	readable_data = FOREACH unique_person_data GENERATE myfuncs.clear_data(unique_identifier.subject, predicate, p_object);
	readable_data = FOREACH readable_data GENERATE tuple_0.subject, tuple_0.predicate, tuple_0.p_object;
	DUMP readable_data;
	""")
	params = {'name': sys.argv[1]}
	bound = P.bind(params)
	stats = bound.runSingle()
	if not stats.isSuccessful():
		raise 'failed'
	print '-------------------------------------------------------------------------------------------------------------------------------------'
	iterator = stats.result("readable_data").iterator()
	while not is_final(iterator):
		identifiers = find_object_identifiers(iterator)
		print identifiers
		# pig join
		break
	