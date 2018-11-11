from collections import namedtuple
from baseconv import base62
import random


Fact = namedtuple("Fact", 'name body entity_id fact_type transaction_id', defaults=['str', ''])

def dict_to_facts(dicto):
    dicto_copy = {**dicto}
    entity_id = dicto_copy.pop('id')
    newfacts = [Fact(transaction_id = '',
                     entity_id = entity_id,
                     name = key,
                     fact_type = 'TEXT',
                     body = val) for key, val in dicto_copy.items()]
    return newfacts

Transaction = namedtuple("Transaction", 'transaction_id entity_id timestamp')


def genID(thing):
    return '{a}id{b}'.format(a = thing.upper()[slice(0, 3)], b = random64())

def random64():
    return base62.encode(random.randint(9, 999999999999999999999999))