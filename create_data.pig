-- register my UDF in python
register '/usr/local/pig_data/my_functions.py' using jython as myfuncs;

identifier_data = LOAD '/usr/local/pig_data/persons.txt' USING PigStorage('\t') AS (subject:chararray, object:chararray);

data = LOAD '/usr/local/pig_data/freebase.gz' USING PigStorage('\t') AS (subject:chararray, predicate:chararray, object:chararray);
data = LIMIT data 2500000;
data = FILTER data BY (predicate MATCHES '.*http://rdf.freebase.com.*') AND NOT (object MATCHES '.*http://rdf.freebase.com/ns/base.*') AND NOT (predicate == '<http://rdf.freebase.com/ns/type.object.key>') AND NOT (predicate == '<http://rdf.freebase.com/key/dataworld.freeq>') AND NOT (predicate == '<http://rdf.freebase.com/key/authority.netflix.api>') AND NOT (object == '<http://rdf.freebase.com/ns/common.topic>') AND NOT (predicate == '<http://rdf.freebase.com/key/user.hangy.viaf>') AND NOT (predicate == '<http://rdf.freebase.com/key/authority.us.gov.loc.na>') AND NOT (predicate == '<http://rdf.freebase.com/key/wikipedia.en_id>') AND NOT (predicate == '<http://rdf.freebase.com/key/authority.musicbrainz>') AND NOT (object == '<http://rdf.freebase.com/ns/base.type_ontology.physically_instantiable>') AND NOT (object == '<http://rdf.freebase.com/ns/people.person>') AND NOT (object == '<http://rdf.freebase.com/ns/common.notable_for>');

-- OBJECT NAMES -d
object_name_data = DISTINCT(FILTER data BY (predicate == '<http://rdf.freebase.com/ns/type.object.name>'));
object_name_data = GROUP object_name_data BY subject;
object_name_data = FOREACH object_name_data GENERATE myfuncs.filter_data(group, object_name_data);
object_name_data = FOREACH object_name_data GENERATE tuple_0.subject, tuple_0.object;

-- DESCRIPTION -d
description_data = DISTINCT(FILTER data BY (predicate == '<http://rdf.freebase.com/ns/common.topic.description>'));
description_data = GROUP description_data BY subject;
description_data = FOREACH description_data GENERATE myfuncs.filter_data(group, description_data);
description_data = FOREACH description_data GENERATE tuple_0.subject, tuple_0.object;

-- OBJECT TYPE DATA -d 
object_type_data = FILTER data BY (predicate == '<http://rdf.freebase.com/ns/type.object.type>');
object_type_data = DISTINCT(FOREACH object_type_data GENERATE myfuncs.extract_type(subject, object));
object_type_data = FOREACH object_type_data GENERATE tuple_0.subject, tuple_0.object;
object_type_data = FOREACH (GROUP object_type_data BY subject) GENERATE FLATTEN(group), object_type_data.(object) AS object_type;

-- DATE OF BIRTH -d
birth_date_data = DISTINCT(FILTER data BY (predicate == '<http://rdf.freebase.com/ns/people.person.date_of_birth>'));
birth_date_data = DISTINCT(FOREACH birth_date_data GENERATE myfuncs.clear_data(subject, object));
birth_date_data = FOREACH birth_date_data GENERATE tuple_0.subject, tuple_0.object;

-- PLACE OF BIRTH -d
birth_place_data = FILTER data BY (predicate == '<http://rdf.freebase.com/ns/people.person.place_of_birth>');
birth_place_data = DISTINCT(FOREACH birth_place_data GENERATE subject, object);
birth_place_data = JOIN birth_place_data BY object, object_name_data BY subject;
birth_place_data = FOREACH birth_place_data GENERATE $0, $3;

-- WEBPAGES -d
webpages_data = FILTER data BY (predicate == '<http://rdf.freebase.com/ns/common.topic.topic_equivalent_webpage>');
webpages_data = DISTINCT(FOREACH webpages_data GENERATE subject, object);
webpages_data = FOREACH (GROUP webpages_data BY subject) GENERATE FLATTEN(group), webpages_data.(object) AS webpages;

-- DATE OF DEATH -d
death_date_data = DISTINCT(FILTER data BY (predicate == '<http://rdf.freebase.com/ns/people.deceased_person.date_of_death>'));
death_date_data = DISTINCT(FOREACH death_date_data GENERATE myfuncs.clear_data(subject, object));
death_date_data = FOREACH death_date_data GENERATE tuple_0.subject, tuple_0.object;

-- PLACE OF DEATH -d
death_place_data = FILTER data BY (predicate == '<http://rdf.freebase.com/ns/people.deceased_person.place_of_death>');
death_place_data = DISTINCT(FOREACH death_place_data GENERATE subject, object);
death_place_data = JOIN death_place_data BY object, object_name_data BY subject;
death_place_data = FOREACH death_place_data GENERATE $0, $3;

-- PLACE OF BURIAL -d
burial_place_data = FILTER data BY (predicate == '<http://rdf.freebase.com/ns/people.deceased_person.place_of_burial>');
burial_place_data = DISTINCT(FOREACH burial_place_data GENERATE subject, object);
burial_place_data = JOIN burial_place_data BY object, object_name_data BY subject;
burial_place_data = FOREACH burial_place_data GENERATE $0, $3;

-- PROMINENT TYPE -d
prominent_type_data = FILTER data BY (predicate == '<http://rdf.freebase.com/ns/kg.object_profile.prominent_type>');
prominent_type_data = DISTINCT(FOREACH prominent_type_data GENERATE myfuncs.extract_type(subject, object));
prominent_type_data = FOREACH prominent_type_data GENERATE tuple_0.subject, tuple_0.object;
prominent_type_data = FOREACH (GROUP prominent_type_data BY subject) GENERATE FLATTEN(group), prominent_type_data.(object) AS prominent_type;

-- NOTABLE FOR -d
notable_data = FILTER data BY (predicate == '<http://rdf.freebase.com/ns/common.topic.notable_for>');
notable_data = DISTINCT(FOREACH notable_data GENERATE subject, object);
notable_data = JOIN notable_data BY object, object_name_data BY subject;
notable_data = FOREACH notable_data GENERATE $0, $3;

-- ALIAS -d
alias_data = FILTER data BY (predicate == '<http://rdf.freebase.com/ns/common.topic.alias>');
alias_data = DISTINCT(FOREACH alias_data GENERATE myfuncs.clear_data(subject, object));
alias_data = FOREACH alias_data GENERATE tuple_0.subject, tuple_0.object;
alias_data = FOREACH (GROUP alias_data BY subject) GENERATE FLATTEN(group), alias_data.(object) AS alias_type;

-- PROFESSION -d
profession_data = FILTER data BY (predicate == '<http://rdf.freebase.com/ns/people.person.profession>');
profession_data = DISTINCT(FOREACH profession_data GENERATE subject, object);
profession_data = JOIN profession_data BY object, object_name_data BY subject;
profession_data = FOREACH profession_data GENERATE $0, $3;

-- NATIONALITY -d
nationality_data = FILTER data BY (predicate == '<http://rdf.freebase.com/ns/people.person.nationality>');
nationality_data = DISTINCT(FOREACH nationality_data GENERATE subject, object);
nationality_data = JOIN nationality_data BY object, object_name_data BY subject;
nationality_data = FOREACH nationality_data GENERATE $0, $3;

-- WEBPAGES -d
topical_web_data = FILTER data BY (predicate == '<http://rdf.freebase.com/ns/common.topic.topical_webpage>');
topical_web_data = DISTINCT(FOREACH topical_web_data GENERATE subject, object);
topical_web_data = FOREACH (GROUP topical_web_data BY subject) GENERATE FLATTEN(group), topical_web_data.(object) AS topic_web;

-- NOTABLE TYPE -d
notable_type_data = FILTER data BY (predicate == '<http://rdf.freebase.com/ns/common.topic.notable_types>');
notable_type_data = DISTINCT(FOREACH notable_type_data GENERATE subject, object);
notable_type_data = JOIN notable_type_data BY object, object_name_data BY subject;
notable_type_data = FOREACH notable_type_data GENERATE $0, $3;

-- WEIGHT -d
weight_data = FILTER data BY (predicate == '<http://rdf.freebase.com/ns/people.person.weight_kg>');
weight_data = FOREACH weight_data GENERATE myfuncs.clear_data(subject, object);
weight_data = FOREACH weight_data GENERATE tuple_0.subject, tuple_0.object;

-- HEIGHT -d
height_data = FILTER data BY (predicate == '<http://rdf.freebase.com/ns/people.person.height_meters>');
height_data = FOREACH height_data GENERATE myfuncs.clear_data(subject, object);
height_data = FOREACH height_data GENERATE tuple_0.subject, tuple_0.object;