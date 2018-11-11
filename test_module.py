import wurmstore
from contextlib import contextmanager
from wurmstore import WurmStore, Fact, dict_to_facts, genID

testdata = {
    'people': [
        {'id' : 'asdf', 'name': 'Jon', 'age': 34, 'fruit': 'pear'},
        {'id': '123123', 'name': 'Sean', 'age': 2, 'fruit': 'strawberry'},
        {'id': 'qwerty', 'name': 'Jan', 'age': 19, 'fruit': 'watermelon'}
    ]
}

def test_dict_to_facts():
    entity = testdata['people'][0]
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
    w = WurmStore('sqlite', 'db.sqlite')
    person = testdata['people'][0]
    asfacts = dict_to_facts(person)
    insertion = w.insert(asfacts)
    assert insertion.transaction.entity_id == person['id']

@contextmanager
def withTestingStore():
    w = WurmStore('memory')
    yield w

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

