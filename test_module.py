import wurmstore
from contextlib import contextmanager
from wurmstore import WurmStore, Fact, dict_to_facts, genID, facts_to_entity, group_facts_under_entity_id
import wurmstore.facts as wf
import time

testdata = [
        {'id' : 'asdf', 'name': 'Jon', 'age': 34, 'fruit': 'pear', 'category': 'people'},
        {'id': '123123', 'name': 'Sean', 'age': 2, 'fruit': 'strawberry', 'category': 'people'},
        {'id': 'qwerty', 'name': 'Jan', 'age': 19, 'fruit': 'watermelon', 'category': 'people'},
        {'id' : 'fdsafdsa', 'setby': 'asdf', 'table': '2b', 'time': '2018-12-03 13:55:00', 'category': 'reservations'},
        {'id' : 'edxrfvtgb', 'setby': 'asdf', 'table': '46a', 'time': '2018-12-07 18:30:00', 'category': 'reservations'},
        {'id' : '09876', 'setby': 'qwerty', 'table': '2b', 'time': '2018-11-01 22:00:00', 'category': 'reservations'}
]

def test_dict_to_facts():
    entity = testdata[0]
    entity_copy = {**entity}

    newfacts = dict_to_facts(entity)
    for fact in newfacts:
        assert isinstance(fact, Fact)
        assert fact.entity_id == entity['id']
        assert fact.body == entity[fact.name]
    entity_copy.pop('id')
    assert len(entity_copy.items()) == len(newfacts)

def test_gen_id():
    assert genID('Transaction')[slice(0, 5)] == 'TRAid'

def test_insert():
    w = WurmStore('memory')
    person = testdata[0]
    asfacts = dict_to_facts(person)
    insertion = w.insert(asfacts)
    assert not insertion.error
    assert insertion.results[0].entity_id == person['id']


def setup_inserted_db():
    #w = WurmStore('sqlite_naive', 'db_inserted.sqlite')
    w = WurmStore('memory')
    [w.insert(x) for x in testdata]
    return w 

def setup_updated_db():
    #w = WurmStore('sqlite_naive', 'db_updated.sqlite')
    w = WurmStore('memory')
    [w.insert(x) for x in testdata]
    time.sleep(0.4)  
    w.insert([ 
        w.Fact(name='fruit', body='banana', entity_id='123123'),
        w.Fact(name='age', body='21', fact_type='INT', entity_id='qwerty') 
    ])
    time.sleep(0.2) 
    return w

incorrectly_formed_query = {}

def test_incorrect_query():
    w = setup_inserted_db()
    bad_read_result = w.read(search_query = incorrectly_formed_query)
    assert isinstance(bad_read_result.error, TypeError)
    assert not bad_read_result.results

get_all_people_query = {
    'find': ['*'],
    'where': {'category': 'people'} 
}

def test_general_reads():
    w = setup_inserted_db()
    people_read_result = w.read(search_query = {**get_all_people_query})
    if people_read_result.error:
        raise people_read_result.error
    assert people_read_result.error is None
    assert people_read_result.results
    assert isinstance(people_read_result.results, list)
    entities = w.facts_to_dicts(people_read_result.results)
    assert entities.get(testdata[0]['id'], None)
    print(entities)
    assert entities[testdata[0]['id']] == testdata[0]

def test_id_reads():
    get_one_person_query = {
        'find': ["*"],
        'count': 1,
        'where': {'id': testdata[2]['id']}
    }
    w = setup_inserted_db()
    id_read_result = w.read(get_one_person_query)
    if id_read_result.error:
        raise id_read_result.error
    assert id_read_result.results

def test_db_updates(): 
    w = setup_updated_db()
    people_read_result = w.read(search_query = {**get_all_people_query})
    if people_read_result.error:
        raise people_read_result.error
    entities = w.facts_to_dicts(people_read_result.results)
    assert entities.get('123123', None)['fruit'] == 'banana'
    assert entities.get('qwerty', None)['age'] == 21
    assert len(people_read_result.results) == 12

def test_finding_individual_facts_with_query():
    w = setup_inserted_db()
    single_property_query = {'find': ['fruit', 'name'], 'where': {'category': 'people'}}
    single_property_result = w.read(single_property_query)
    if single_property_result.error:
        raise single_property_result.error
    assert len(single_property_result.results) == 6

def test_getting_full_raw():
    w = setup_updated_db()
    raw_data = w.get_raw_data()
    wi = setup_inserted_db()
    raw_wi_data = wi.get_raw_data()
    factos = list(raw_data['facts'])
    print(factos)
    if w.store_type == 'memory':
        assert len(factos) - 2 is len(list(raw_wi_data['facts']))
        assert len(list(raw_data['transactions'])) - 1 is len(list(raw_wi_data['transactions']))

def test_restoring_raw_data():
    w = setup_updated_db()
    wn = WurmStore('memory')
    #wn = WurmStore('sqlite_naive', 'db_updated.sqlite')  
    w_raw = w.get_raw_data()
    wn.restore_from_raw(w_raw)
    orig_entities = w.facts_to_dicts(w.read(get_all_people_query).results)
    rest_entities = w.facts_to_dicts(wn.read(get_all_people_query).results)
    print("orig entities" + str(orig_entities))
    print("rest entities" + str(rest_entities))
    assert orig_entities == rest_entities

def test_just_to_print():
    w = setup_inserted_db()
    print(w)
    #assert False
    assert True


def test_thing(): 
    assert True

def no(): 
    assert False

def test_other():
    a = 1
    b = 3
    assert a == a 

def test_imported():
    assert WurmStore()
