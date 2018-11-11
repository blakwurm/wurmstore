import sqlite3
from wurmstore.facts import Fact, Transaction, dict_to_facts


print('module loaded')

sql = """

"""

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

    def __insert_facts__(self, facts):
        pass
    
    def open_transaction(self):
        return ''
    
    def __deconstruct_dict(self, dicto):
        return dict_to_facts(dicto)
    
    def insert(self, entity):
        entity_facts = dict_to_facts(entity) if isinstance(entity, dict) else entity
        pass
