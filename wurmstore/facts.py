from collections import namedtuple
from contextlib import contextmanager
from baseconv import base62
import random
import time


Fact = namedtuple("Fact", 'name body entity_id fact_type transaction_id', defaults=['str', ''])

Transaction = namedtuple("Transaction", 'transaction_id entity_id timestamp')

Insertion = namedtuple("Insertion", 'transaction facts successful', defaults=[True])

Head = namedtuple("Head", "entity_id transaction_id")

ReadResult = namedtuple("ReadResult", 'results error')

def dict_to_facts(entity, transaction = Transaction('', '', time.time())):
    dicto_copy = {**entity}
    entity_id = ''
    try:
        entity_id = dicto_copy.pop('id')
    except:
        entity_id = dicto_copy.pop('entity_id')
    newfacts = [Fact(transaction_id = transaction.transaction_id,
                     entity_id = entity_id,
                     name = key,
                     fact_type = 'TEXT',
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
       search_query['count']
       search_query['where']
       assert isinstance(search_query['count'], int)
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

def facts_to_entity(facts):
    print("facts are " + facts)
    raw = {x.name: x.body for x in facts}
    return {**raw, **{'id': facts[0].entity_id}}

