#!/usr/bin/python

# Run command: pig -x local /usr/local/pig_data/python_exe.py "'Katrin Marras'"

# vyhodnocovanie -> priemer osoby vlastnosti / najviac / najmenej
# doplnit parenta (identifier) pre jednotlive node-y - pre korektne retazenie

from org.apache.pig.scripting import *
import sys

# first version of class to hold data of person
# data are in tree structure so one node have all its childs in list
class DataTreeView:
	def __init__(self, identifier, subject):
		self.identifier = identifier
		self.subject = subject
		self.data = []
		self.childs = []

# get first identifier from iterator
def get_first_identifier(i_iterator):
	for pig_tuple in identifier_iterator:
		return pig_tuple.get(0)
	
#load data into current node and return founded identifiers
def load_data_and_ids(d_iterator, c_node):
	identifiers = []
	for pig_tuple in d_iterator:
		predicate = str(pig_tuple.get(1))
		p_object = str(pig_tuple.get(2))
		if 'rdf.freebase.com' in p_object:
			identifiers.append((p_object, predicate))
			continue
		is_present = False
		for one_data in c_node.data:
			if one_data[0] == predicate and one_data[1] == p_object:
				is_present = True
		if is_present == False:
			c_node.data.append((predicate, p_object))
	return identifiers

# run data extraction for one node (identifier)
# recurrent function until new identifiers are found
def run_single(curr_node, read_identifiers):
	# load data for selected identifier (in first run person identifier)
	P = Pig.compile("""
	register '/usr/local/pig_data/my_functions.py' using jython as myfuncs;
	-- load all data and identifiers of persons
	all_data = LOAD '/usr/local/pig_data/freebase.gz' USING PigStorage('\t') AS (subject:chararray, predicate:chararray, object:chararray);
	all_data = LIMIT all_data 200000000;
	-- filter all data to include freebase name_space and exclude base data
	filtered_data = FILTER all_data BY (predicate MATCHES '.*http://rdf.freebase.com.*') AND NOT(object MATCHES '.*http://rdf.freebase.com/ns/base.*') AND (subject == '$identifier');
	readable_data = FOREACH filtered_data GENERATE myfuncs.clear_data(subject, predicate, object);
	readable_data = FOREACH readable_data GENERATE tuple_0.subject, tuple_0.predicate, tuple_0.p_object;
	DUMP readable_data;
	""")
	params = {'identifier': curr_node.identifier}
	bound = P.bind(params)
	stats = bound.runSingle()
	if not stats.isSuccessful():
		raise 'failed'
		
	# create data iterator to hold loaded data and insert this data into current node
	data_iterator = stats.result("readable_data").iterator()
	new_identifiers = load_data_and_ids(data_iterator, curr_node)

	# create nodes for every new identifier (only if it was not add into read identifiers already) and append them into read_identifiers
	for one_identifier in new_identifiers:
		is_saved = False
		for saved_identifier in read_identifiers:
			if one_identifier[0] == saved_identifier[0] and one_identifier[1] == saved_identifier[1]:
				is_saved = True
		if is_saved == False:
			print "--------------------------------------------------------------------------------------------------------------------------------"
			print "--------------------------------------------------------------------------------------------------------------------------------"
			print "--------------------------------------------------------------------------------------------------------------------------------"
			print one_identifier[0] + " => " + one_identifier[1]
			print "--------------------------------------------------------------------------------------------------------------------------------"
			print "--------------------------------------------------------------------------------------------------------------------------------"
			print "--------------------------------------------------------------------------------------------------------------------------------"
			new_node = DataTreeView(one_identifier[0], one_identifier[1])
			read_identifiers.append((one_identifier[0], one_identifier[1]))
			curr_node.childs.append(new_node)
			run_single(new_node, read_identifiers)

# print tree data after data extraction is complete
def print_tree_data(curr_node, indentation):
	print '\t' * indentation + curr_node.subject
	for one_data in curr_node.data:
		print '\t' * (indentation + 1) + one_data[0] + ' => ' + one_data[1]
	for child in curr_node.childs:
		print_tree_data(child, indentation + 1)

if __name__ == '__main__':
	# load data to extract identifier of person
	P = Pig.compile("""
	identifier_data = LOAD '/usr/local/pig_data/persons.txt' USING PigStorage('\t') AS (subject:chararray, object:chararray);
	-- find identifier of selected person
	identifier = DISTINCT(FILTER identifier_data BY (object == '$name'));
	identifier = LIMIT identifier 1;
	DUMP identifier;
	""")
	params = {'name': sys.argv[1]}
	bound = P.bind(params)
	stats = bound.runSingle()
	if not stats.isSuccessful():
		raise 'failed'
	
	print "--------------------------------------------------------------------------------------------------------------------------------"
	print "--------------------------------------------------------------------------------------------------------------------------------"
	print "--------------------------------------------------------------------------------------------------------------------------------"
	# list of already visited identifiers	
	checked_identifiers = []
	
	# get first identifier - identifier of person
	identifier_iterator = stats.result("identifier").iterator()
	first_identifier = get_first_identifier(identifier_iterator)
	
	# add first identifier to the list of visited identifiers
	checked_identifiers.append((first_identifier, sys.argv[1]))
	
	# create first node in tree view
	first_node = DataTreeView(first_identifier, sys.argv[1])
	
	# run function to collect data of identifier
	# recursive function so all childs will also collect their data
	# creates whole tree structure
	run_single(first_node, checked_identifiers)
	
	print "--------------------------------------------------------------------------------------------------------------------------------"
	print "--------------------------------------------------------------------------------------------------------------------------------"
	print "--------------------------------------------------------------------------------------------------------------------------------"
	
	# print data from first_node
	print_tree_data(first_node, 0)
	