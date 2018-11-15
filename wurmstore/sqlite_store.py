import sqlite3
from wurmstore.facts import *
from contextlib import contextmanager
from collections import Counter
import time



table_creation_sql = [
    """
create table if not exists facts (
    name text,
    body text,
    entity_id text,
    fact_type text,
    fact_operation text,
    transaction_id text,
    timestamp integer
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
            return [c.execute('insert into facts values (?, ?, ?, ?, ?, ?, ?)', x) for x in facts]
    
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
        return InsertionResult(results = insertion.facts, error = None)
    
    def create_transaction(self, entity_id):
        return Transaction(transaction_id = genID('transaction'), entity_id = entity_id, timestamp = get_now_in_millis())
    
    def Fact(self, *, name, body, entity_id, fact_type="TEXT", fact_operation = 'ADD', transaction_id="", timestamp=0):
        return Fact(name=name, body=body, entity_id=entity_id, fact_type=fact_type, fact_operation=fact_operation, transaction_id=transaction_id)
        
    def __deconstruct_dict(self, entity, transaction):
        return dict_to_facts(entity, transaction)
    
    def __prepare_for_insertion__(self, entity, transaction):
        if isinstance(entity, dict):
            return dict_to_facts(entity=entity, transaction=transaction)
        elif isinstance(entity, list):
            return [Fact._make(x)._replace(transaction_id = transaction.transaction_id, timestamp = transaction.timestamp) for x in entity]
        else:
            raise TypeError('entity should be a dict, or a list of lists/tuples')
    
    def insert(self, entity):
        try:
            transaction = self.create_transaction(getID(entity))
            #self.__insert_head__(transaction)
            entity_facts = self.__prepare_for_insertion__(entity, transaction)
            insert = Insertion(transaction = transaction, facts = entity_facts, successful = True)
            return self.__insert_insertion__(insert)
        except Exception as e:
            #return Insertion(transaction = None, facts = None, successful = False)
            return InsertionResult(results = [], error = e)
    
    def __prepare_query_and_plug__(self, search_query):
        param_query = {}
        raw_plugs = []
        for i, (key, value) in enumerate(search_query['where'].items()):
            if key == 'id':
                raw_plugs.append('entity_id = :id')
                param_query.update({'id': value})
            else:
                param_query.update({'where_name{a}'.format(a=i): key})
                param_query.update({'where_body{a}'.format(a=i): value})
                raw_plugs.append('(name = :where_name{a} and body = :where_body{a})'.format(a=i))
        param_query.update({'__timestamp': search_query.get('timestamp', get_now_in_millis())})
        plug = 'and '.join(raw_plugs)
        print(plug)
        print(param_query)
        return (param_query, plug)

    
    def __get_facts_for__(self, search_query):
        sql_revised_find = """
        select * from facts where 
        entity_id in (
            select entity_id from (
                select entity_id, name, body from facts where timestamp < :__timestamp group by entity_id, name
            )
            where {plug}
        ) 
        and not fact_operation = 'REMOVE'
        and timestamp < :__timestamp group by entity_id, name
        """
        query_dict, param_plug = self.__prepare_query_and_plug__(search_query)
        with self.__with_cursor() as c:
            full_query = sql_revised_find.format(plug = param_plug)
            print(full_query)
            c.execute(full_query, query_dict)
            raw_facts = c.fetchall()
            converted_facts = [Fact._make(x) for x in raw_facts]
            print(raw_facts)
            facts = converted_facts
        return facts
    
    def __get_transactions_for__(self, ids):
        sqlstring = "select * from transactions where entityID = ?"

    def __read__(self, search_query):
        return ReadResult(results = self.__get_facts_for__(search_query), error = None)

    def read(self, search_query):
        if (not query_well_formed(search_query)):
            return ReadResult(results = [], error = TypeError('search_query must be formed correctly'))
        try:
            return self.__read__(search_query)
        except Exception as e:
            return ReadResult(results = [], error = e)
    
    def delete(self, facts):
        """Will mark the provided facts as deleted"""
        try:
            "a"
        except Exception as e:
            return DeleteResult(results = [], error = e)