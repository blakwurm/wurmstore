from collections import namedtuple
from contextlib import contextmanager
from baseconv import base62
import random
import time


Fact = namedtuple("Fact", 'name body entity_id fact_type fact_operation transaction_id timestamp', defaults=['str', 'ADD', '', 0])

Transaction = namedtuple("Transaction", 'transaction_id timestamp')

Insertion = namedtuple("Insertion", 'transaction facts successful', defaults=[True])

Head = namedtuple("Head", "entity_id transaction_id")

InsertionResult = namedtuple("InsertionResult", 'results error')

ReadResult = namedtuple("ReadResult", 'results error')

DeleteResult = namedtuple("DeleteResult", 'results error')

def get_now_in_millis():
    return int(time.time() * 1000)

def get_fact_body_type(fact_body):
    if isinstance(fact_body, int):
        return "INT"
    elif isinstance(fact_body, float):
        return "FLOAT"
    else:
        return "TEXT"

def dict_to_facts(entity, transaction = Transaction('', get_now_in_millis())):
    dicto_copy = {**entity}
    entity_id = ''
    try:
        entity_id = dicto_copy.pop('id') 
    except:
        entity_id = dicto_copy.pop('entity_id')
    newfacts = [Fact(transaction_id = transaction.transaction_id,
                     entity_id = entity_id,
                     name = key,
                     fact_type = get_fact_body_type(val),
                     timestamp = transaction.timestamp,
                     body = val) 
                for key, val 
                in dicto_copy.items()]
    return newfacts

def getID(entity):
    try:
        return entity.get('id', entity.get('entity_id', ''))
    except:
        return entity[0].entity_id

def genID(thing):
    return '{a}id{b}'.format(a = thing.upper()[slice(0, 3)], b = random64())

def random64():
    return base62.encode(random.randint(9, 999999999999999999999999))

def query_well_formed(search_query):
    try:
       search_query['find']
       search_query['where']
       assert isinstance(search_query.get('count', 0), int)
       assert isinstance(search_query['where'], dict) or isinstance(search_query['where'], list)
       return True
    except:
       return False

def ids_from_facts(facts):
    raw_ids = [x.entity_id for x in facts]
    return set(raw_ids)

def group_facts_under_entity_id(facts):
    ids = {x.entity_id for x in facts}
    raw_entities = {y: [x for x in facts if x.entity_id == y] for y in ids}
    entities = {y: facts_to_entity(x) for y, x in raw_entities}
    return entities
 
fact_body_convertion = {
    'TEXT': lambda a: a,
    'INT': int,
    'FLOAT': float
}

def convert_fact_body(fact):
    return fact_body_convertion[fact.fact_type](fact.body)

def facts_to_entity(facts):
    print("facts are " + facts)
    raw = {x.name: convert_fact_body(x) for x in facts}
    print('raw entity is ' + str(raw))
    return {**raw, **{'id': facts[0].entity_id}}
