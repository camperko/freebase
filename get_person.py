from elasticsearch import Elasticsearch
import textwrap
import json

index_name = 'pig_data_two'
wrapper = textwrap.TextWrapper(width=60)


class PigData:
    def __init__(self):
        self.name = '-'
        self.gender = []
        self.name_aliases = []
        self.nationality = []
        self.profession = []
        self.birth_date = []
        self.birth_place = []
        self.death_date = []
        self.death_place = []
        self.burial_place = []
        self.type_information = []
        self.description = []
        self.weight = []
        self.height = []
        self.webpages = []

    def return_rec(self):
        rec = {'name': self.name, 'gender': self.gender, 'name_aliases': self.name_aliases,
               'nationality': self.nationality, 'profession': self.profession, 'birth_date': self.birth_date,
               'birth_place': self.birth_place, 'death_date': self.death_date, 'death_place': self.death_place,
               'burial_place': self.burial_place, 'type_information': self.type_information,
               'description': self.description, 'weight': self.weight, 'height': self.height, 'webpages': self.webpages}
        return json.dumps(rec)

    def append_data(self, index, result):
        if index == 1:
            self.gender = result
        elif index == 2:
            self.name_aliases = result
        elif index == 3:
            self.nationality = result
        elif index == 4:
            self.profession = result
        elif index == 5:
            self.birth_date = result
        elif index == 6:
            self.birth_place = result
        elif index == 7:
            self.death_date = result
        elif index == 8:
            self.death_place = result
        elif index == 9:
            self.burial_place = result
        elif index == 10:
            self.type_information = result
        elif index == 11:
            self.description = result
        elif index == 12:
            self.weight = result
        elif index == 13:
            self.height = result
        elif index == 14:
            self.webpages = result


def connect_elasticsearch():
    _es = None
    _es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    if _es.ping():
        print('Connected')
    else:
        print('Error connecting')
    return _es


def create_index(es_object):
    created = False
    settings = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        },
        "mappings": {
            "dynamic": "strict",
            "properties": {
                "name": {
                    "type": "text"
                },
                "gender": {
                    "type": "nested",
                    "properties": {
                        "one_gender": {
                            "type": "text"
                        }
                    }
                },
                "name_aliases": {
                    "type": "nested",
                    "properties": {
                        "one_alias": {
                            "type": "text"
                        }
                    }
                },
                "nationality": {
                    "type": "nested",
                    "properties": {
                        "one_nationality": {
                            "type": "text"
                        }
                    }
                },
                "profession": {
                    "type": "nested",
                    "properties": {
                        "one_profession": {
                            "type": "text"
                        }
                    }
                },
                "birth_date": {
                    "type": "nested",
                    "properties": {
                        "one_birth_date": {
                            "type": "text"
                        }
                    }
                },
                "birth_place": {
                    "type": "nested",
                    "properties": {
                        "one_birth_place": {
                            "type": "text"
                        }
                    }
                },
                "death_date": {
                    "type": "nested",
                    "properties": {
                        "one_death_date": {
                            "type": "text"
                        }
                    }
                },
                "death_place": {
                    "type": "nested",
                    "properties": {
                        "one_death_place": {
                            "type": "text"
                        }
                    }
                },
                "burial_place": {
                    "type": "nested",
                    "properties": {
                        "one_burial_place": {
                            "type": "text"
                        }
                    }
                },
                "type_information": {
                    "type": "nested",
                    "properties": {
                        "one_info": {
                            "type": "text"
                        }
                    }
                },
                "description": {
                    "type": "nested",
                    "properties": {
                        "one_description": {
                            "type": "text"
                        }
                    }
                },
                "weight": {
                    "type": "nested",
                    "properties": {
                        "one_weight": {
                            "type": "float"
                        }
                    }
                },
                "height": {
                    "type": "nested",
                    "properties": {
                        "one_height": {
                            "type": "float"
                        }
                    }
                },
                "webpages": {
                    "type": "nested",
                    "properties": {
                        "one_page": {
                            "type": "text"
                        }
                    }
                },
            }
        }
    }
    try:
        if not es_object.indices.exists(index_name):
            result = es_object.indices.create(index=index_name, ignore=400, body=settings)
            print('Created Index')
            print(result)
        created = True
    except Exception as ex:
        print(str(ex))
    finally:
        return created


def resolve_outer_type(index):
    if index == 1:
        return 'gender'
    elif index == 2:
        return 'name_aliases'
    elif index == 3:
        return 'nationality'
    elif index == 4:
        return 'profession'
    elif index == 5:
        return 'birth_date'
    elif index == 6:
        return 'birth_place'
    elif index == 7:
        return 'death_date'
    elif index == 8:
        return 'death_place'
    elif index == 9:
        return 'burial_place'
    elif index == 10:
        return 'type_information'
    elif index == 11:
        return 'description'
    elif index == 12:
        return 'weight'
    elif index == 13:
        return 'height'
    elif index == 14:
        return 'webpages'


def resolve_search_pair(index):
    if index == 1:
        return ['name']
    elif index == 2:
        return ['alias', 'name_aliases', 'one_alias']
    elif index == 3:
        return ['nationality', 'nationality', 'one_nationality']
    elif index == 4:
        return ['profession', 'profession', 'one_profession']
    elif index == 5:
        return ['birth date', 'birth_date', 'one_birth_date']
    elif index == 6:
        return ['birth place', 'birth_place', 'one_birth_place']
    elif index == 7:
        return ['date of death', 'death_date', 'one_death_date']
    elif index == 8:
        return ['place of death', 'death_place', 'one_death_place']
    elif index == 9:
        return ['place of burial', 'burial_place', 'one_burial_place']
    elif index == 10:
        return ['type information', 'type_information', 'one_info']
    elif index == 11:
        return ['description', 'description', 'one_description']
    elif index == 12:
        return ['weight', 'weight', 'one_weight']
    elif index == 13:
        return ['height', 'height', 'one_height']
    elif index == 14:
        return ['gender', 'gender', 'one_gender']
    elif index == 15:
        return ['webpages', 'webpages', 'one_page']


def resolve_type(index):
    if index == 1:
        return 'gender'
    elif index == 2:
        return 'alias'
    elif index == 3:
        return 'nationality'
    elif index == 4:
        return 'profession'
    elif index == 5:
        return 'birth_date'
    elif index == 6:
        return 'birth_place'
    elif index == 7:
        return 'death_date'
    elif index == 8:
        return 'death_place'
    elif index == 9:
        return 'burial_place'
    elif index == 10:
        return 'info'
    elif index == 11:
        return 'description'
    elif index == 12:
        return 'weight'
    elif index == 13:
        return 'height'
    elif index == 14:
        return 'page'


def resolve_inner_type(index):
    return 'one_' + resolve_type(index)


def parse(l):
    one_record = PigData()
    columns = l.split("\t")
    one_record.name = columns[0]
    for index, column in enumerate(columns[1:]):
        result = []
        if column == '' or column == '\n':
            if index + 1 == 12 or index + 1 == 13:
                result.append({resolve_inner_type(index + 1): -1})
            else:
                result.append({resolve_inner_type(index + 1): '-'})
        else:
            inner_column = column[2:-2]
            if '),(' in inner_column:
                column_parts = inner_column.split("),(")
                for part in column_parts:
                    if index + 1 == 12 or index + 1 == 13:
                        result.append({resolve_inner_type(index + 1): float(part)})
                    else:
                        result.append({resolve_inner_type(index + 1): part})
            else:
                if index + 1 == 12 or index + 1 == 13:
                    result.append({resolve_inner_type(index + 1): float(inner_column)})
                else:
                    result.append({resolve_inner_type(index + 1): inner_column})
        one_record.append_data(index + 1, result)

    return one_record.return_rec()


def save_person(elastic_object, record):
    is_stored = True
    try:
        elastic_object.index(index=index_name, body=record)
    except Exception as ex:
        print('ERROR')
        print(str(ex))
        is_stored = False
    finally:
        return is_stored


def create_name_query(target_name):
    search_name_query = {
        'match_phrase_prefix': {
            'name': target_name
        }
    }
    return json.dumps(search_name_query)


def create_nested_query(target_value, target_part_index):
    target_name = resolve_search_pair(int(target_part_index))
    inner_query = target_name[1] + '.' + target_name[2]
    nested_query = {
        'nested': {
            'path': target_name[1],
            'query': {
                'bool': {
                    'must': [
                        {
                            'match_phrase_prefix': {
                                inner_query: target_value
                            }
                        }
                    ]
                }
            }
        }
    }
    return json.dumps(nested_query)


def create_multiple_nested_query(inputs, parts):
    name_query = None
    result_nested_query = ''
    if '1' in parts:
        name_query = create_name_query(inputs[parts.index('1')])
        inputs.pop(parts.index('1'))
        parts.pop(parts.index('1'))

    for index, part in enumerate(parts):
        if index == 0:
            result_nested_query = create_nested_query(inputs[index], part)
        else:
            result_nested_query = result_nested_query + ',' + create_nested_query(inputs[index], part)

    if name_query is not None:
        if result_nested_query != '':
            result_nested_query = result_nested_query + ',' + name_query
        else:
            result_nested_query = name_query
    result_query = {
        'query': {
            'bool': {
                'must': [
                    json.loads(result_nested_query)
                ]
            }
        }
    }
    return json.dumps(result_query)


def response_value(person_object, keys):
    result = []
    for item in person_object[keys[1]]:
        if item[keys[2]] != '-' and item[keys[2]] != -1:
            result.append(item[keys[2]])
    return result


def print_one_attribute(attribute, person_object, index):
    print(attribute + ': ')
    for value in response_value(person_object, resolve_search_pair(index)):
        if index == 12:
            print('\t' + str(value) + 'kg')
        elif index == 13:
            print('\t' + str(value) + 'm')
        elif index == 11:
            value = value.replace('\\n', ' ').replace('\\t', ' ').replace('\\"', '\"')
            word_list = wrapper.wrap(text=value)
            for element in word_list:
                print('\t' + element)
        else:
            value = value.replace('\\n', ' ').replace('\\t', ' ').replace('\\"', '\"')
            print('\t' + value)


def print_person(person_object):
    print('Name: ')
    print('\t' + person_object['name'])
    print_one_attribute('Aliases', person_object, 2)
    print_one_attribute('Gender', person_object, 14)
    print_one_attribute('Nationality', person_object, 3)
    print_one_attribute('Profession', person_object, 4)
    print_one_attribute('Birth date', person_object, 5)
    print_one_attribute('Birth place', person_object, 6)
    print_one_attribute('Date of death', person_object, 7)
    print_one_attribute('Place of death', person_object, 8)
    print_one_attribute('Place of burial', person_object, 9)
    print_one_attribute('Information', person_object, 10)
    print_one_attribute('Description', person_object, 11)
    print_one_attribute('Weight', person_object, 12)
    print_one_attribute('Height', person_object, 13)
    print_one_attribute('Related webpages', person_object, 15)
    print('--------------------------------------------------------------------')


def find_person(es_object, search_req):
    res = es_object.search(index=index_name, body=search_req)
    result_object = res['hits']['hits']
    print(res)
    for result in result_object:
        print_person(result['_source'])
    if res['hits']['total']['value'] > 10:
        print('!!!Too many values (' + str(res['hits']['total']['value']) + '), please better specify query!!!')


if __name__ == '__main__':
    es = connect_elasticsearch()
    if (es is not None) and create_index(es):
        # Part of code to add extracted persons into elastic index
        # for i in range(7):
        #     name = 'part-r-0000' + str(i)
        #     print(name)
        #     with open('res/'+name,  encoding='utf-8') as infile:
        #         for line in infile:
        #             parsed_line = parse(line)
        #             succ_stored = save_person(es, parsed_line)
        #             if not succ_stored:
        #                 exit()

        # Part of code to get persons by given attribute
        print('Freebase person database')
        while True:
            print('Select search criterion')
            for i in range(11):
                print(str(i + 1) + ' Search by ' + resolve_search_pair(i + 1)[0])
            user_choice = input('Select number: ')
            choice_parts = user_choice.split(' ')
            search_inputs = []
            for choice_part in choice_parts:
                user_input = input('Insert search expression for ' + resolve_search_pair(int(choice_part))[0] + ': ')
                search_inputs.append(user_input)
            print('--------------------------------------------------------------------')
            search_object = create_multiple_nested_query(search_inputs, choice_parts)
            find_person(es, search_object)
            should_continue = input('Continue? (y/n): ')
            if should_continue == 'n' or should_continue == 'N':
                exit()
