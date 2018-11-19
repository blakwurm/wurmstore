import sqlite3
from wurmstore.facts import *
from wurmstore.wurmstorebase import WurmStoreBase
from contextlib import contextmanager
from collections import Counter
import time


class SQLiteStore(WurmStoreBase):

    def __init__(self, location, *, memory_db = False):
        WurmStoreBase.__init__(self, store_type = 'memory' if memory_db else 'sqlite_naive', store_location = location)
        self._conn = sqlite3.connect(location)
        for x in table_creation_sql:
            self._conn.execute(x)

    @contextmanager
    def _with_cursor_(self):
        yield self._conn.cursor()
        self._conn.commit()

    def _insert_facts_(self, facts):
        with self._with_cursor_() as c:
            return [c.execute('insert or ignore into facts values (?, ?, ?, ?, ?, ?, ?)', x) for x in facts]
    
    def _insert_transactions_(self, transactions):
        with self._with_cursor_() as c:
            return [c.execute('insert or ignore into transactions values (?, ?)', x) for x in transactions]

    def _insert_insertion_(self, insertion):
        self._insert_transactions_([insertion.transaction])
        self._insert_facts_(insertion.facts)
        return InsertionResult(results = insertion.facts, error = None)
    
    def _deconstruct_dict_(self, entity, transaction):
        return dict_to_facts(entity, transaction)
    
    def _prepare_query_and_plug_(self, search_query):
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
        # Add 1 to now timestamp, as otherwise we can't get stuff that was added this milisecond
        param_query.update({'__timestamp': search_query.get('timestamp', get_now_in_millis() + 1)})
        plug = 'and '.join(raw_plugs)
        return (param_query, plug)
    
    def _prepare_find_plug_(self, search_query):
        if '*' in set(search_query['find']):
            return ({}, '')
        else:
            find_query = {'find_name{a}'.format(a=i): findname for i, findname in enumerate(search_query['find'])}
            find_plug = 'where name = :find_name0' if len(find_query) is 1 else 'where name = ' + ' or name = '.join([':find_name{a}'.format(a=i) for i in range(len(search_query['find']))])
            return (find_query, find_plug)

    _sql_find_query_ = """
            select * from (
                select * from facts where 
                    entity_id in (
                        select entity_id from (
                            select * from (
                                select entity_id, name, body from facts where timestamp  < :__timestamp order by timestamp asc
                            ) group by entity_id, name
                        )
                        where {plug}
                    )    
                and not fact_operation = 'REMOVE'
                and timestamp < :__timestamp group by entity_id, name
            )
            {find_plug}
        """
        
    def _get_facts_for_(self, search_query):
        sql_revised_find = self._sql_find_query_ 
        query_dict, param_plug = self._prepare_query_and_plug_(search_query)
        find_dict, find_plug = self._prepare_find_plug_(search_query)
        full_params = {**query_dict, **find_dict}
        with self._with_cursor_() as c:
            full_query = sql_revised_find.format(plug = param_plug, find_plug = find_plug)
            print(full_params)
            print(full_query)
            c.execute(full_query, full_params)
            raw_facts = c.fetchall()
            converted_facts = [Fact._make(x) for x in raw_facts]
            facts = converted_facts
        return facts

    def _read_(self, search_query):
        return ReadResult(results = self._get_facts_for_(search_query), error = None)
    
    def _get_raw_facts_(self, timestamp_begin = 0, timestamp_end = 999999999999999):
        sqlstatement = 'select * from facts where timestamp > :timestamp_begin and timestamp < :timestamp_end'
        # TODO - Make more memory-safe
        with self._with_cursor_() as c:
            c.execute(sqlstatement, {'timestamp_begin': timestamp_begin, 'timestamp_end': timestamp_end})
            for thing in c:
                yield thing
    
    def _get_raw_transactions_(self):
        sqlstatement = 'select * from transactions'
        with self._with_cursor_() as c:
            c.execute(sqlstatement)
            for thing in c:
                yield thing
    


table_creation_sql = [
    """
create table if not exists facts (
    name text,
    body text,
    entity_id text,
    fact_type text,
    fact_operation text,
    transaction_id text,
    timestamp integer,
    primary key (name, entity_id, transaction_id, timestamp) 
) without rowid;
    """,
"""
create table if not exists transactions (
    transaction_id text primary key,
    timestamp integer
);
"""
]
