import sqlite3
from wurmstore.facts import Fact, Transaction, Insertion, dict_to_facts, genID, getID
from contextlib import contextmanager
import time


print('module loaded')

table_creation_sql = [
    """
create table if not exists facts (
    name text,
    body blob,
    entity_id text,
    fact_type text,
    transaction_id text
);
    """,
"""
create table if not exists transactions (
    transaction_id text primary key,
    entity_id text,
    timestamp integer
);
"""
]

class SQLiteStore:
    def __init__(self, location):
        self.__conn = sqlite3.connect(location)
        for x in table_creation_sql:
            self.__conn.execute(x)

    @contextmanager
    def __with_cursor(self):
        yield self.__conn.cursor()
        self.__conn.commit()

    def __insert_facts__(self, facts):
        with self.__with_cursor() as c:
            return [c.execute('insert into facts values (?, ?, ?, ?, ?)', x) for x in facts]
    
    def __insert_transaction__(self, transaction):
        with self.__with_cursor() as c:
            c.execute('insert into transactions values (?, ?, ?)', transaction)
    
    def create_transaction(self, entity_id):
        return Transaction(transaction_id = genID('transaction'), entity_id = entity_id, timestamp = int(time.time()))
        
    def __deconstruct_dict(self, dicto):
        return dict_to_facts(dicto)
    
    def insert(self, entity):
        transaction = self.create_transaction(entity[0].entity_id)
        entity_facts = dict_to_facts(entity=entity, transaction=transaction) if isinstance(entity, dict) else [x._replace(transaction_id = transaction.transaction_id) for x in entity]
        insert = Insertion(transaction = transaction, facts = entity_facts)
        self.__insert_transaction__(transaction)
        self.__insert_facts__(entity_facts)
        return insert
