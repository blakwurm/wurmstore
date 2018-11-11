import wurmstore
from contextlib import contextmanager
from wurmstore import WurmStore, Fact, dict_to_facts, genID

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
    assert insertion.successful
    assert insertion.transaction.entity_id == person['id']


def setup_inserted_db():
    #w = WurmStore('sqlite', 'db.sqlite')
    w = WurmStore('memory')
    [w.insert(x) for x in testdata]
    return w

incorrectly_formed_query = {}

def test_freshly_inserted_reads():
    get_all_people_query = {
        'find': '*',
        'count': 1,
        'where': {'category': 'people'} 
    }
    w = setup_inserted_db()
    assert isinstance(w.read(search_query = incorrectly_formed_query).error, TypeError)
    people_read_result = w.read(search_query = {**get_all_people_query})
    assert people_read_result.error is None


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

