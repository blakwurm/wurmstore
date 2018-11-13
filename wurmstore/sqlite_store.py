import sqlite3
from wurmstore.facts import Fact, Transaction, Insertion, Head, ReadResult, dict_to_facts, genID, getID, query_well_formed, ids_from_facts, facts_to_entity, group_facts_under_entity_id
from contextlib import contextmanager
from collections import Counter
import time



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
""",
"""
create table if not exists heads (
    entity_id text primary key,
    transaction_id text
)
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
    
    def __insert_head__(self, transaction):
        head = Head(entity_id = transaction.entity_id, transaction_id = transaction.transaction_id)
        with self.__with_cursor() as c:
            return c.execute('insert into heads values (?, ?)', head)
        
    def __insert_insertion__(self, insertion):
        self.__insert_transaction__(insertion.transaction)
        self.__insert_facts__(insertion.facts)
        return insertion
    
    def create_transaction(self, entity_id):
        return Transaction(transaction_id = genID('transaction'), entity_id = entity_id, timestamp = int(time.time()))
        
    def __deconstruct_dict(self, entity, transaction):
        return dict_to_facts(entity, transaction)
    
    def insert(self, entity):
        try:
            transaction = self.create_transaction(getID(entity))
            #self.__insert_head__(transaction)
            entity_facts = None
            if isinstance(entity, dict):
                entity_facts = self.__deconstruct_dict(entity=entity, transaction=transaction)
            else:
                entity_facts = [x._replace(transaction_id = transaction.transaction_id) for x in entity]
            insert = Insertion(transaction = transaction, facts = entity_facts, successful = True)
            return self.__insert_insertion__(insert)
        except:
            return Insertion(transaction = None, facts = None, successful = False)
    

    def __get_facts_for__(self, search_query):
        sqlstring = "select * from facts where name = ? AND body = ?"
        facts = []
        with self.__with_cursor() as c:
            fact_sublists = [c.execute(sqlstring, [qname, qbody]) for qname,qbody in search_query['where'].items()]
            raw_facts = [item for sublist in fact_sublists for item in sublist]
            facts = [Fact._make(x) for x in raw_facts]
        return facts
    
    def __get_transactions_for__(self, ids):
        sqlstring = "select * from transactions where entityID = ?"


    def read(self, search_query):
        if (query_well_formed(search_query)):
            try:
                facts = self.__get_facts_for__(search_query)
                ids = ids_from_facts(facts)
                self.__get_transactions_for__(ids)
                # todo - add bit where facts are condensed into an entity
                facts.sort(key=lambda a: a.entity_id)
                return ReadResult(results = facts, error = None)
            except Exception as e:
                return ReadResult(results = [], error = e)
        else:
            return ReadResult(results = [], error = TypeError())
        pass
    
    def read_group_by_entity(self, search_query):
        readresult = self.read(search_query)
        ids = {x.entity_id for x in readresult.results}
