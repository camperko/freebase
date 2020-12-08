/*
	Script to extract selected data from freebase
	Run command: pig -x local /usr/local/pig_data/create_data.pig
	Run command: pig -x mapreduce hdfs://localhost:9000/pig_data/create_data.pig
*/

-- register my UDF in python
register 'hdfs://localhost:9000/python_functions/my_functions.py' using jython as myfuncs;

identifier_data = LOAD 'hdfs://localhost:9000/person_output/part-m-00000' USING PigStorage('\t') AS (subject:chararray, object:chararray);

data = LOAD 'hdfs://localhost:9000/freebase_data/freebase.gz' USING PigStorage('\t') AS (subject:chararray, predicate:chararray, object:chararray);
data = FILTER data BY (predicate MATCHES '.*http://rdf.freebase.com.*') AND NOT (object MATCHES '.*http://rdf.freebase.com/ns/base.*') AND NOT (predicate == '<http://rdf.freebase.com/ns/type.object.key>') AND NOT (predicate == '<http://rdf.freebase.com/key/dataworld.freeq>') AND NOT (predicate == '<http://rdf.freebase.com/key/authority.netflix.api>') AND NOT (object == '<http://rdf.freebase.com/ns/common.topic>') AND NOT (predicate == '<http://rdf.freebase.com/key/user.hangy.viaf>') AND NOT (predicate == '<http://rdf.freebase.com/key/authority.us.gov.loc.na>') AND NOT (predicate == '<http://rdf.freebase.com/key/wikipedia.en_id>') AND NOT (predicate == '<http://rdf.freebase.com/key/authority.musicbrainz>') AND NOT (object == '<http://rdf.freebase.com/ns/base.type_ontology.physically_instantiable>') AND NOT (object == '<http://rdf.freebase.com/ns/people.person>') AND NOT (object == '<http://rdf.freebase.com/ns/common.notable_for>');

-- OBJECT NAMES -d
object_name_data = DISTINCT(FILTER data BY (predicate == '<http://rdf.freebase.com/ns/type.object.name>'));
object_name_data = GROUP object_name_data BY subject;
object_name_data = FOREACH object_name_data GENERATE myfuncs.filter_data(group, object_name_data);
object_name_data = FOREACH object_name_data GENERATE tuple_0.subject, tuple_0.object;

-- GENDER -d
gender_data = FILTER data BY (predicate == '<http://rdf.freebase.com/ns/people.person.gender>');
gender_data = DISTINCT(FOREACH gender_data GENERATE subject, object);
gender_data = JOIN gender_data BY object, object_name_data BY subject;
gender_data = FOREACH gender_data GENERATE $0, $3;
gender_data = FOREACH (GROUP gender_data BY subject) GENERATE FLATTEN(group) AS subject, gender_data.(object) AS gender_type;

-- NAMES 
names_data = FILTER data BY (predicate == '<http://rdf.freebase.com/ns/type.object.name>');
names_data = DISTINCT(FOREACH names_data GENERATE myfuncs.clear_data(subject, object));
names_data = FOREACH names_data GENERATE tuple_0.subject, tuple_0.object;
names_data = FOREACH (GROUP names_data BY subject) GENERATE FLATTEN(group) AS subject, names_data.(object) AS name_type;

-- ALIAS -d
alias_data = FILTER data BY (predicate == '<http://rdf.freebase.com/ns/common.topic.alias>');
alias_data = DISTINCT(FOREACH alias_data GENERATE myfuncs.clear_data(subject, object));
alias_data = FOREACH alias_data GENERATE tuple_0.subject, tuple_0.object;
alias_data = FOREACH (GROUP alias_data BY subject) GENERATE FLATTEN(group) AS subject, alias_data.(object) AS alias_type;

-- MERGED NAMES
merged_names_data = JOIN names_data BY subject FULL OUTER, alias_data BY subject;
merged_names_data = FOREACH merged_names_data GENERATE myfuncs.join_bags($0, $1, $2, $3);
merged_names_data = FOREACH merged_names_data GENERATE tuple_0.subject, tuple_0.merged_data;

-- NATIONALITY -d
nationality_data = FILTER data BY (predicate == '<http://rdf.freebase.com/ns/people.person.nationality>');
nationality_data = DISTINCT(FOREACH nationality_data GENERATE subject, object);
nationality_data = JOIN nationality_data BY object, object_name_data BY subject;
nationality_data = FOREACH nationality_data GENERATE $0, $3;
nationality_data = FOREACH (GROUP nationality_data BY subject) GENERATE FLATTEN(group) AS subject, nationality_data.(object) AS nationality_type;

-- PROFESSION -d
profession_data = FILTER data BY (predicate == '<http://rdf.freebase.com/ns/people.person.profession>');
profession_data = DISTINCT(FOREACH profession_data GENERATE subject, object);
profession_data = JOIN profession_data BY object, object_name_data BY subject;
profession_data = FOREACH profession_data GENERATE $0, $3;
profession_data = FOREACH (GROUP profession_data BY subject) GENERATE FLATTEN(group) AS subject, profession_data.(object) AS profession_type;

-- DATE OF BIRTH -d
birth_date_data = DISTINCT(FILTER data BY (predicate == '<http://rdf.freebase.com/ns/people.person.date_of_birth>'));
birth_date_data = DISTINCT(FOREACH birth_date_data GENERATE myfuncs.clear_data(subject, object));
birth_date_data = FOREACH birth_date_data GENERATE tuple_0.subject, tuple_0.object;
birth_date_data = FOREACH (GROUP birth_date_data BY subject) GENERATE FLATTEN(group) AS subject, birth_date_data.(object) AS birth_date_type;

-- PLACE OF BIRTH -d
birth_place_data = FILTER data BY (predicate == '<http://rdf.freebase.com/ns/people.person.place_of_birth>');
birth_place_data = DISTINCT(FOREACH birth_place_data GENERATE subject, object);
birth_place_data = JOIN birth_place_data BY object, object_name_data BY subject;
birth_place_data = FOREACH birth_place_data GENERATE $0, $3;
birth_place_data = FOREACH (GROUP birth_place_data BY subject) GENERATE FLATTEN(group) AS subject, birth_place_data.(object) AS birth_place_type;

-- DATE OF DEATH -d
death_date_data = DISTINCT(FILTER data BY (predicate == '<http://rdf.freebase.com/ns/people.deceased_person.date_of_death>'));
death_date_data = DISTINCT(FOREACH death_date_data GENERATE myfuncs.clear_data(subject, object));
death_date_data = FOREACH death_date_data GENERATE tuple_0.subject, tuple_0.object;
death_date_data = FOREACH (GROUP death_date_data BY subject) GENERATE FLATTEN(group) AS subject, death_date_data.(object) AS death_date_type;

-- PLACE OF DEATH -d
death_place_data = FILTER data BY (predicate == '<http://rdf.freebase.com/ns/people.deceased_person.place_of_death>');
death_place_data = DISTINCT(FOREACH death_place_data GENERATE subject, object);
death_place_data = JOIN death_place_data BY object, object_name_data BY subject;
death_place_data = FOREACH death_place_data GENERATE $0, $3;
death_place_data = FOREACH (GROUP death_place_data BY subject) GENERATE FLATTEN(group) AS subject, death_place_data.(object) AS death_place_type;

-- PLACE OF BURIAL -d
burial_place_data = FILTER data BY (predicate == '<http://rdf.freebase.com/ns/people.deceased_person.place_of_burial>');
burial_place_data = DISTINCT(FOREACH burial_place_data GENERATE subject, object);
burial_place_data = JOIN burial_place_data BY object, object_name_data BY subject;
burial_place_data = FOREACH burial_place_data GENERATE $0, $3;
burial_place_data = FOREACH (GROUP burial_place_data BY subject) GENERATE FLATTEN(group) AS subject, burial_place_data.(object) AS burial_place_type;

-- OBJECT TYPE DATA -d
object_type_data = FILTER data BY (predicate == '<http://rdf.freebase.com/ns/type.object.type>');
object_type_data = DISTINCT(FOREACH object_type_data GENERATE myfuncs.extract_type(subject, object));
object_type_data = FOREACH object_type_data GENERATE tuple_0.subject, tuple_0.object;
object_type_data = FOREACH (GROUP object_type_data BY subject) GENERATE FLATTEN(group) AS subject, object_type_data.(object) AS object_type;

-- PROMINENT TYPE -d
prominent_type_data = FILTER data BY (predicate == '<http://rdf.freebase.com/ns/kg.object_profile.prominent_type>');
prominent_type_data = DISTINCT(FOREACH prominent_type_data GENERATE myfuncs.extract_type(subject, object));
prominent_type_data = FOREACH prominent_type_data GENERATE tuple_0.subject, tuple_0.object;
prominent_type_data = FOREACH (GROUP prominent_type_data BY subject) GENERATE FLATTEN(group) AS subject, prominent_type_data.(object) AS prominent_type;

-- NOTABLE FOR -d
notable_data = FILTER data BY (predicate == '<http://rdf.freebase.com/ns/common.topic.notable_for>');
notable_data = DISTINCT(FOREACH notable_data GENERATE subject, object);
notable_data = JOIN notable_data BY object, object_name_data BY subject;
notable_data = FOREACH notable_data GENERATE $0, $3;
notable_data = FOREACH (GROUP notable_data BY subject) GENERATE FLATTEN(group) AS subject, notable_data.(object) AS notable_for;

-- NOTABLE TYPE -d
notable_type_data = FILTER data BY (predicate == '<http://rdf.freebase.com/ns/common.topic.notable_types>');
notable_type_data = DISTINCT(FOREACH notable_type_data GENERATE subject, object);
notable_type_data = JOIN notable_type_data BY object, object_name_data BY subject;
notable_type_data = FOREACH notable_type_data GENERATE $0, $3;
notable_type_data = FOREACH (GROUP notable_type_data BY subject) GENERATE FLATTEN(group) AS subject, notable_type_data.(object) AS notable_type;

-- MERGED TYPE
merged_type_data = JOIN object_type_data BY subject FULL OUTER, prominent_type_data BY subject;
merged_type_data = FOREACH merged_type_data GENERATE myfuncs.join_bags($0, $1, $2, $3);
merged_type_data = FOREACH merged_type_data GENERATE tuple_0.subject, tuple_0.merged_data;

-- MERGED TYPE 2
merged_type_data = JOIN merged_type_data BY subject FULL OUTER, notable_data BY subject;
merged_type_data = FOREACH merged_type_data GENERATE myfuncs.join_bags($0, $1, $2, $3);
merged_type_data = FOREACH merged_type_data GENERATE tuple_0.subject, tuple_0.merged_data;

-- MERGED TYPE ALL
merged_type_data = JOIN merged_type_data BY subject FULL OUTER, notable_type_data BY subject;
merged_type_data = FOREACH merged_type_data GENERATE myfuncs.join_bags($0, $1, $2, $3);
merged_type_data = FOREACH merged_type_data GENERATE tuple_0.subject, tuple_0.merged_data;

-- DESCRIPTION -d
description_data = DISTINCT(FILTER data BY (predicate == '<http://rdf.freebase.com/ns/common.topic.description>'));
description_data = GROUP description_data BY subject;
description_data = FOREACH description_data GENERATE myfuncs.filter_data(group, description_data);
description_data = FOREACH description_data GENERATE tuple_0.subject, tuple_0.object;
description_data = FOREACH (GROUP description_data BY subject) GENERATE FLATTEN(group) AS subject, description_data.(object) AS description_type;

-- WEIGHT -d
weight_data = FILTER data BY (predicate == '<http://rdf.freebase.com/ns/people.person.weight_kg>');
weight_data = FOREACH weight_data GENERATE myfuncs.clear_data(subject, object);
weight_data = FOREACH weight_data GENERATE tuple_0.subject, tuple_0.object;
weight_data = FOREACH (GROUP weight_data BY subject) GENERATE FLATTEN(group) AS subject, weight_data.(object) AS weight_type;

-- HEIGHT -d
height_data = FILTER data BY (predicate == '<http://rdf.freebase.com/ns/people.person.height_meters>');
height_data = FOREACH height_data GENERATE myfuncs.clear_data(subject, object);
height_data = FOREACH height_data GENERATE tuple_0.subject, tuple_0.object;
height_data = FOREACH (GROUP height_data BY subject) GENERATE FLATTEN(group) AS subject, height_data.(object) AS height_type;

-- WEBPAGES -d
webpages_data = FILTER data BY (predicate == '<http://rdf.freebase.com/ns/common.topic.topic_equivalent_webpage>');
webpages_data = DISTINCT(FOREACH webpages_data GENERATE subject, object);
webpages_data = FOREACH (GROUP webpages_data BY subject) GENERATE FLATTEN(group) AS subject, webpages_data.(object) AS webpages;

-- WEBPAGES TOPICAL -d
topical_web_data = FILTER data BY (predicate == '<http://rdf.freebase.com/ns/common.topic.topical_webpage>');
topical_web_data = DISTINCT(FOREACH topical_web_data GENERATE subject, object);
topical_web_data = FOREACH (GROUP topical_web_data BY subject) GENERATE FLATTEN(group) AS subject, topical_web_data.(object) AS topic_web;

-- MERGED WEB -d
merged_web_data = JOIN webpages_data BY subject FULL OUTER, topical_web_data BY subject;
merged_web_data = FOREACH merged_web_data GENERATE myfuncs.join_bags($0, $1, $2, $3);
merged_web_data = FOREACH merged_web_data GENERATE tuple_0.subject, tuple_0.merged_data;

-- FINAL DATA
final_data = FOREACH(JOIN identifier_data BY subject LEFT OUTER, gender_data BY subject) GENERATE $0, $1, $3;
final_data = FOREACH(JOIN final_data BY $0 LEFT OUTER, merged_names_data BY subject) GENERATE $0, $1, $2, $4;
final_data = FOREACH(JOIN final_data BY $0 LEFT OUTER, nationality_data BY subject) GENERATE $0, $1, $2, $3, $5;
final_data = FOREACH(JOIN final_data BY $0 LEFT OUTER, profession_data BY subject) GENERATE $0, $1, $2, $3, $4, $6;
final_data = FOREACH(JOIN final_data BY $0 LEFT OUTER, birth_date_data BY subject) GENERATE $0, $1, $2, $3, $4, $5, $7;
final_data = FOREACH(JOIN final_data BY $0 LEFT OUTER, birth_place_data BY subject) GENERATE $0, $1, $2, $3, $4, $5, $6, $8;
final_data = FOREACH(JOIN final_data BY $0 LEFT OUTER, death_date_data BY subject) GENERATE $0, $1, $2, $3, $4, $5, $6, $7, $9;
final_data = FOREACH(JOIN final_data BY $0 LEFT OUTER, death_place_data BY subject) GENERATE $0, $1, $2, $3, $4, $5, $6, $7, $8, $10;
final_data = FOREACH(JOIN final_data BY $0 LEFT OUTER, burial_place_data BY subject) GENERATE $0, $1, $2, $3, $4, $5, $6, $7, $8, $9, $11;
final_data = FOREACH(JOIN final_data BY $0 LEFT OUTER, merged_type_data BY subject) GENERATE $0, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $12;
final_data = FOREACH(JOIN final_data BY $0 LEFT OUTER, description_data BY subject) GENERATE $0, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $13;
final_data = FOREACH(JOIN final_data BY $0 LEFT OUTER, weight_data BY subject) GENERATE $0, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $14;
final_data = FOREACH(JOIN final_data BY $0 LEFT OUTER, height_data BY subject) GENERATE $0, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $15;
final_data = FOREACH(JOIN final_data BY $0 LEFT OUTER, merged_web_data BY subject) GENERATE $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $16;

STORE final_data INTO 'hdfs://localhost:9000/final_output/' USING PigStorage('\t');

